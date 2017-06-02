from datetime import datetime

from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, func, ForeignKey, MetaData
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from croniter import croniter

metadata = MetaData()
BaseModel = declarative_base(metadata=metadata)

## TODO: refactor Workflow and Task to share scheduling fields and methods?

class Workflow(BaseModel):
    __tablename__ = 'workflows'

    name = Column(String, primary_key=True)
    active = Column(Boolean, nullable=False)
    title = None
    description = None
    concurrency = None
    sla = None
    schedule = None
    start_date = None
    end_date = None

    _tasks = None

    def __init__(
        self,
        name=None,
        active=False,
        title=None,
        description=None,
        concurrency=1,
        sla=None,
        schedule=None,
        start_date=None,
        end_date=None):

        self.name = name
        if not self.name:
            raise Exception('`name` required for Workflow')

        self.active = active
        self.title = title
        self.description = description
        self.concurrency = concurrency
        self.sla = sla

        self.schedule = schedule
        self.start_date = start_date
        self.end_date = end_date

        self._tasks = set()

    def __repr__(self):
        return '<Workflow name: {} active: {}>'.format(self.name, self.active)

    def refresh(self, session):
        persisted = session.query(Workflow).filter(Workflow.name == self.name).first()
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

    def get_dependencies_graph(self):
        graph = dict()
        for task in self._tasks:
            graph[task.name] = task._dependencies
        return graph

    def get_task(self, task_name):
        for task in self._tasks:
            if task.name == task_name:
                return task

class Task(BaseModel):
    __tablename__ = 'tasks'

    name = Column(String, primary_key=True)
    active = Column(Boolean, nullable=False)
    title = None
    description = None
    concurrency = None
    max_retries = None
    timeout = None
    sla = None
    schedule = None
    start_date = None
    end_date = None
    fn = None

    _dependencies = None

    def __init__(
        self,
        workflow=None,
        name=None,
        active=False,
        title=None,
        description=None,
        concurrency=1,
        max_retries=1,
        timeout=300,
        sla=None,
        schedule=None,
        start_date=None,
        end_date=None,
        fn=None):

        self.name = name
        if not self.name:
            raise Exception('`name` required for Task')

        self.workflow = workflow
        if self.workflow:
            ## TODO: warn when task already in tasks?
            self.workflow._tasks.add(self)

        self.active = active
        self.title = title
        self.description = description
        self.concurrency = concurrency
        self.max_retries = max_retries
        self.timeout = timeout
        self.sla = sla

        self.schedule = schedule
        self.start_date = start_date
        self.end_date = end_date

        self.fn = fn

        self._dependencies = set()

    def __repr__(self):
        return '<Task name: {} active: {}>'.format(self.name, self.active)

    def refresh(self, session):
        persisted = session.query(Task).filter(Task.name == self.name).first()
        self.active = persisted.active

    def depends_on(self, task):
        ## TODO: warn when task already in _dependencies?
        self._dependencies.add(task.name)

    def next_run(self, base_time=None):
        if not base_time:
            base_time = self.start_date
        iter = croniter(self.schedule, base_time)
        return iter.get_next(datetime)

class Taskflow(object):
    _workflows = dict()
    _tasks = dict()

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

    def get_fresh_tasks(self, session):
        for task in self._tasks.values():
            task.refresh(session)
        return self._tasks.values()

    def add_task(self, task):
        if task.workflow:
            raise Exception('Tasks with workflows are not added individually, just add the workflow')
        self._tasks.add(task)

class WorkflowInstance(BaseModel):
    __tablename__ = 'workflow_instances'

    id = Column(BigInteger, primary_key=True)
    workflow = Column(String, nullable=False)
    scheduled = Column(Boolean) ## TODO: nullable=False ? default ?
    run_at = Column(DateTime) ## TODO: nullable=False ?
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    status = Column(String, nullable=False)
    params = Column(JSONB)
    created_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now())
    updated_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now(),
                        onupdate=func.now())

class TaskInstance(BaseModel):
    __tablename__ = 'task_instances'

    id = Column(BigInteger, primary_key=True)
    task = Column(String, nullable=False)
    workflow_instance = Column(BigInteger, ForeignKey('workflow_instances.id'))
    status = Column(String, nullable=False)
    run_at = Column(DateTime) ## TODO: nullable=False ?
    started_at = Column(DateTime)
    ended_at = Column(DateTime)
    params = Column(JSONB)
    attempts = Column(Integer) ## TODO: default 0 ? nullable=False ?
    created_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now())
    updated_at = Column(DateTime,
                        nullable=False,
                        server_default=func.now(),
                        onupdate=func.now())

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
