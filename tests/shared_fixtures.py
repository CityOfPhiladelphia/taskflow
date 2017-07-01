import os
import logging
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pytest

os.environ['CSRF_SECRET'] = 'test'

from taskflow import Taskflow, Workflow, WorkflowInstance, Task, TaskInstance
from taskflow.core.models import BaseModel, User
from taskflow.rest.app import create_app

def get_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

@pytest.fixture(scope='session')
def engine():
    return create_engine('postgresql://localhost/taskflow_test')

@pytest.fixture
def tables(engine):
    BaseModel.metadata.create_all(engine)
    yield
    BaseModel.metadata.drop_all(engine)

@pytest.fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # put back the connection to the connection pool
    connection.close()

@pytest.fixture
def workflows(dbsession):
    workflow1 = Workflow(name='workflow1', active=True, schedule='0 6 * * *')
    workflow2 = Workflow(name='workflow2', active=True)
    dbsession.add(workflow1)
    dbsession.add(workflow2)
    dbsession.commit()

    task1 = Task(workflow=workflow1, name='task1', active=True)
    task2 = Task(workflow=workflow1, name='task2', active=True)
    task3 = Task(workflow=workflow1, name='task3', active=True)
    task4 = Task(workflow=workflow1, name='task4', active=True)

    task3.depends_on(task1)
    task3.depends_on(task2)
    task4.depends_on(task3)

    dbsession.add(task1)
    dbsession.add(task2)
    dbsession.add(task3)
    dbsession.add(task4)

    dbsession.commit()
    return [workflow1, workflow2]

@pytest.fixture
def instances(dbsession, workflows):
    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        started_at=datetime(2017, 6, 3, 6),
        status='running',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()
    task_instance1 = TaskInstance(
        task_name='task1',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 12),
        started_at=datetime(2017, 6, 3, 6, 0, 12),
        ended_at=datetime(2017, 6, 3, 6, 0, 18),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance2 = TaskInstance(
        task_name='task2',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 20),
        started_at=datetime(2017, 6, 3, 6, 0, 20),
        ended_at=datetime(2017, 6, 3, 6, 0, 27),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance3 = TaskInstance(
        task_name='task3',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 28),
        started_at=datetime(2017, 6, 3, 6, 0, 28),
        ended_at=datetime(2017, 6, 3, 6, 0, 32),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance4 = TaskInstance(
        task_name='task4',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='running',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        started_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.add(task_instance3)
    dbsession.add(task_instance4)
    dbsession.commit()

@pytest.fixture
def app(tables, workflows, dbsession):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)
    taskflow.sync_db(dbsession)

    dbsession.add(User(active=True, username='amadonna', password='foo', role='admin'))
    dbsession.commit()

    app = create_app(taskflow, connection_string='postgresql://localhost/taskflow_test', secret_key='foo')

    yield app
