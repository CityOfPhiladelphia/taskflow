from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pytest

from taskflow import Scheduler, Taskflow, Workflow, Task, TaskInstance
from taskflow.core.models import BaseModel

@pytest.fixture(scope='session')
def engine():
    return create_engine('postgresql://localhost/taskflow_test')

@pytest.fixture(scope='session')
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

    print(workflow1)

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

def test_run(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)
    scheduler = Scheduler(dbsession, taskflow, now_override=datetime(2017, 6, 3, 6))
    scheduler.run()
    task_instances = dbsession.query(TaskInstance).all()
    print(task_instances)
