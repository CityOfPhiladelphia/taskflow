from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Boolean,
    Enum,
    func,
    ForeignKey,
    MetaData
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from croniter import croniter

metadata = MetaData()
BaseModel = declarative_base(metadata=metadata)

class Schedulable(object):
    name = Column(String, primary_key=True)
    active = Column(Boolean, nullable=False)

    def __init__(
        self,
        name=None,
        active=False,
        title=None,
        description=None,
        concurrency=1,
        sla=None,
        schedule=None,
        default_priority='normal',
        start_date=None,
        end_date=None):

        self.name = name
        if not self.name:
            raise Exception('`name` required for {}'.format(self.__class__.__name__))

        self.active = active
        self.title = title
        self.description = description
        self.concurrency = concurrency
        self.sla = sla

        self.schedule = schedule
        self.default_priority = default_priority
        self.start_date = start_date
        self.end_date = end_date

    def refresh(self, session):
        recurring_class = self.__class__
        persisted = session.query(recurring_class).filter(recurring_class.name == self.name).first()
        self.active = persisted.active

    def next_run(self, base_time=None):
        if not base_time:
            base_time = datetime.utcnow()
        iter = croniter(self.schedule, base_time)
        return iter.get_next(datetime)

    def last_run(self, base_time=None):
        if not base_time:
            base_time = datetime.utcnow()
        iter = croniter(self.schedule, base_time)
        return iter.get_prev(datetime)

class Workflow(Schedulable, BaseModel):
    __tablename__ = 'workflows'

    _tasks = None

    def __init__(self, *args, **kwargs):
        super(Workflow, self).__init__(*args, **kwargs)

        self._tasks = set()

    def __repr__(self):
        return '<Workflow name: {} active: {}>'.format(self.name, self.active)

    def get_dependencies_graph(self):
        graph = dict()
        for task in self._tasks:
            graph[task.name] = task._dependencies
        return graph

    def get_task(self, task_name):
        for task in self._tasks:
            if task.name == task_name:
                return task

    def get_new_instance(self, scheduled=False, status='queued', run_at=None, priority=None):
        return WorkflowInstance(
            workflow=self.name,
            scheduled=scheduled,
            run_at=run_at or datetime.utcnow(),
            status=status,
            priority=priority or self.default_priority)

class Task(Schedulable, BaseModel):
    __tablename__ = 'tasks'

    def __init__(
        self,
        workflow=None,
        max_retries=1,
        timeout=300,
        params=None,
        push_destination=None,
        fn=None,
        *args, **kwargs):
        super(Task, self).__init__(*args, **kwargs)

        self.workflow = workflow
        if self.workflow:
            if self in self.workflow._tasks:
                raise Exception('`{}` already added to workflow `{}`'.format(self.name, self.workflow.name))
            self.workflow._tasks.add(self)

        self.max_retries = max_retries
        self.timeout = timeout

        self.params = params

        self.push_destination = push_destination
        self.fn = fn

        self._dependencies = set()

    def __repr__(self):
        return '<Task name: {} active: {}>'.format(self.name, self.active)

    def depends_on(self, task):
        if self.workflow == None:
            raise Exception('Task dependencies only work with Workflows')
        if task.name in self._dependencies:
            raise Exception('`{}` already depends on `{}`'.format(self.name, task.name))
        if self.name == task.name:
            raise Exception('A task cannot depend on itself')
        self._dependencies.add(task.name)

    def get_new_instance(self,
                         scheduled=False,
                         status='queued',
                         workflow_instance=None,
                         run_at=None,
                         priority=None):
        return TaskInstance(
            task_name=self.name,
            workflow_instance=workflow_instance,
            scheduled=scheduled,
            push=self.push_destination != None,
            status=status,
            priority=priority or self.default_priority,
            run_at=run_at or datetime.utcnow())

class Taskflow(object):
    def __init__(self):
        self._workflows = dict()
        self._tasks = dict()
        self._push_workers = []

    def add_workflow(self, workflow):
        self._workflows[workflow.name] = workflow

    def add_workflows(self, workflows):
        for workflow in workflows:
            self.add_workflow(workflow)

    def get_workflow(self, workflow_name):
        return self._workflows[workflow_name]

    def get_fresh_workflows(self, session):
        for workflow in self._workflows.values():
            workflow.refresh(session)
        return self._workflows.values()

    def add_task(self, task):
        if task.workflow != None:
            raise Exception('Tasks with workflows are not added individually, just add the workflow')
        self._tasks[task.name] = task

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def get_task(self, task_name):
        return self._tasks[task_name]

    def get_fresh_tasks(self, session):
        for task in self._tasks.values():
            task.refresh(session)
        return self._tasks.values()

    def persist(self, session): ## TODO: make upsert?
        for workflow in self._workflows.values():
            session.add(workflow) ## TODO: workflows tasks as well?
        for task in self._tasks.values():
            session.add(task)
        session.commit()

class SchedulableInstance(object):
    id = Column(BigInteger, primary_key=True)
    scheduled = Column(Boolean, nullable=False)
    run_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    status = Column(Enum('queued',
                         'running',
                         'retrying',
                         'dequeued',
                         'failed',
                         'success',
                         name='taskflow_statuses'),
                    nullable=False)
    priority = Column(Enum('critical',
                           'high',
                           'normal',
                           'low',
                           name='taskflow_priorities'),
                      nullable=False)
    unique = Column(String)
    created_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now())
    updated_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now(),
                        onupdate=func.now())

class WorkflowInstance(SchedulableInstance, BaseModel):
    __tablename__ = 'workflow_instances'

    workflow = Column(String, nullable=False)
    params = Column(JSONB)

    def __repr__(self):
        return '<WorkflowInstance workflow: {} run_at: {} status: {}>'.format(
                    self.workflow,
                    self.run_at,
                    self.status) 

class TaskInstance(SchedulableInstance, BaseModel):
    __tablename__ = 'task_instances'

    ## TODO: add last_heartbeat ?

    task_name = Column(String, nullable=False)
    workflow_instance = Column(BigInteger, ForeignKey('workflow_instances.id'))
    push = Column(Boolean, nullable=False)
    locked_at = Column(DateTime) ## TODO: should workflow instaces have locked_at as well ?
    worker_id = Column(String)
    params = Column(JSONB)
    push_data = Column(JSONB)
    attempts = Column(Integer, nullable=False, default=0)

    def __repr__(self):
        return '<TaskInstance task: {} workflow_instance: {} status: {}>'.format(
                    self.task,
                    self.workflow_instance,
                    self.status)

class TaskflowEvent(BaseModel):
    __tablename__ = 'taskflow_events'

    id = Column(BigInteger, primary_key=True)
    workflow_instance = Column(BigInteger, ForeignKey('workflow_instances.id'))
    task_instance = Column(BigInteger, ForeignKey('task_instances.id'))
    timestamp = Column(DateTime, nullable=False)
    event = Column(String, nullable=False)
    message = Column(String)
