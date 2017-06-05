from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Boolean,
    Enum,
    Index,
    func,
    text,
    ForeignKey,
    MetaData
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base, declared_attr
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
        return session.query(recurring_class).filter(recurring_class.name == self.name).one()

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
            workflow_name=self.name,
            scheduled=scheduled,
            run_at=run_at or datetime.utcnow(),
            status=status,
            priority=priority or self.default_priority)

class Task(Schedulable, BaseModel):
    __tablename__ = 'tasks'

    def __init__(
        self,
        workflow=None,
        retries=0,
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

        self.retries = retries
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
                         workflow_instance_id=None,
                         run_at=None,
                         priority=None,
                         max_attempts=None,
                         timeout=None):
        return TaskInstance(
            task_name=self.name,
            workflow_instance_id=workflow_instance_id,
            scheduled=scheduled,
            push=self.push_destination != None,
            status=status,
            priority=priority or self.default_priority,
            run_at=run_at or datetime.utcnow(),
            max_attempts=max_attempts or self.retries,
            timeout=timeout or self.timeout)

## TODO: add 'pushed' below for timeouts?

pull_sql = """
WITH nextTasks as (
    SELECT id, status, started_at
    FROM task_instances
    WHERE{}
       run_at <= :now
       AND (status = 'queued' OR
             ((status = 'running' OR (status = 'retrying' AND attempts < max_attempts)) AND
              (:now > (locked_at + INTERVAL '1 second' * timeout))))
    ORDER BY
        CASE WHEN priority = 'critical'
             THEN 1
             WHEN priority = 'high'
             THEN 2
             WHEN priority = 'normal'
             THEN 3
             WHEN priority = 'low'
             THEN 4
        END,
        run_at
    LIMIT :max_tasks
    FOR UPDATE SKIP LOCKED
)
UPDATE task_instances SET
    status = 
        (CASE WHEN nextTasks.status = 'running'
              THEN 'retrying'
              ELSE 'running'
         END)::taskflow_statuses,
    worker_id = :worker_id,
    locked_at = :now,
    started_at = COALESCE(nextTasks.started_at, :now),
    attempts = attempts + 1
FROM nextTasks
WHERE task_instances.id = nextTasks.id
RETURNING task_instances.*;
"""

task_names_filter = '\n       task_instances.name = ANY(:task_names)\n       AND'
push_filter = '\n       task_instances.push = true\n       AND'

class Taskflow(object):
    def __init__(self, session):
        self.session = session
        self._workflows = dict()
        self._tasks = dict()
        self._push_workers = dict()

    def add_workflow(self, workflow):
        self._workflows[workflow.name] = workflow

    def add_workflows(self, workflows):
        for workflow in workflows:
            self.add_workflow(workflow)

    def get_workflow(self, workflow_name):
        return self._workflows[workflow_name]

    def get_fresh_workflows(self):
        for workflow_name in self._workflows:
            self._workflows[workflow_name] = self._workflows[workflow_name].refresh(self.session)
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

    def get_fresh_tasks(self):
        for task_name in self._tasks:
            self._tasks[task_name] = self._tasks[task_name].refresh(self.session)
        return self._tasks.values()

    def add_push_worker(self, push_worker):
        self._push_workers[push_worker.push_type] = push_worker

    def get_push_worker(self, push_type):
        return self._push_workers[push_type]

    def persist(self): ## TODO: make upsert?
        for workflow in self._workflows.values():
            self.session.add(workflow) ## TODO: workflows tasks as well?
        for task in self._tasks.values():
            self.session.add(task)
        self.session.commit()

    def pull(self, worker_id, task_names=None, max_tasks=1, now=None, push=False):
        if now == None:
            now = datetime.utcnow()

        params = {
            'worker_id': worker_id,
            'now': now,
            'max_tasks': max_tasks
        }

        filters = ''
        if task_names != None:
            filters += task_names_filter
            params['task_names'] = task_names
        if push:
            filters += push_filter

        pull_sql_with_filters = pull_sql.format(filters)

        task_instances = self.session.query(TaskInstance)\
            .from_statement(text(pull_sql_with_filters))\
            .params(**params)\
            .all()

        return task_instances

class SchedulableInstance(BaseModel):
    __abstract__ = True

    id = Column(BigInteger, primary_key=True)
    scheduled = Column(Boolean, nullable=False)
    run_at = Column(DateTime, nullable=False)
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    status = Column(Enum('queued',
                         'pushed',
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

    def complete(self, session, status, now=None):
        if now == None:
            now = datetime.utcnow()
        self.status = status
        self.ended_at = now
        session.commit()

    def succeed(self, session, now=None):
        self.complete(session, 'success', now=now)

    def fail(self, session, now=None):
        self.complete(session, 'failed', now=now)

class WorkflowInstance(SchedulableInstance):
    __tablename__ = 'workflow_instances'

    workflow_name = Column(String, nullable=False)
    params = Column(JSONB)

    def __repr__(self):
        return '<WorkflowInstance workflow: {} run_at: {} status: {}>'.format(
                    self.workflow_name,
                    self.run_at,
                    self.status)

    # @declared_attr
    # def __table_args__(cls):
    #     print(cls.__name__)
    #     print(cls.workflow_name)
    #     print(cls.unique)
    #     print(cls.status)
    #     return (
    #         Index('index_unique_schedulable',
    #               cls.workflow_name,
    #               cls.unique,
    #               unique=True,
    #               postgresql_where=
    #                 cls.status.in_(['queued','pushed','running','retrying'])),)

class TaskInstance(SchedulableInstance):
    __tablename__ = 'task_instances'

    task_name = Column(String, nullable=False)
    workflow_instance_id = Column(BigInteger, ForeignKey('workflow_instances.id'))
    push = Column(Boolean, nullable=False)
    locked_at = Column(DateTime) ## TODO: should workflow instaces have locked_at as well ?
    worker_id = Column(String)
    params = Column(JSONB)
    push_state = Column(JSONB)
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=1)
    timeout = Column(Integer, nullable=False)

    def __repr__(self):
        return '<TaskInstance task: {} workflow_instance: {} status: {}>'.format(
                    self.task_name,
                    self.workflow_instance_id,
                    self.status)

    # __table_args__ = (
    #     Index('unique_schedulable',
    #           task_name,
    #           SchedulableInstance.unique,
    #           unique=True,
    #           postgresql_where=(
    #             SchedulableInstance.status.in_(['queued','pushed','running','retrying']))),)

class TaskflowEvent(BaseModel):
    __tablename__ = 'taskflow_events'

    id = Column(BigInteger, primary_key=True)
    workflow_instance = Column(BigInteger, ForeignKey('workflow_instances.id'))
    task_instance = Column(BigInteger, ForeignKey('task_instances.id'))
    timestamp = Column(DateTime, nullable=False)
    event = Column(String, nullable=False)
    message = Column(String)
