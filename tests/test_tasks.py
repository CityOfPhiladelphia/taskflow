from datetime import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from taskflow import Scheduler, Taskflow, Task, TaskInstance
from shared_fixtures import *

get_logging()

@pytest.fixture
def tasks(dbsession):
    task1 = Task(name='task1', active=True)
    task2 = Task(name='task2', active=True, schedule='0 6 * * *')
    task3 = Task(name='task3', active=True)
    task4 = Task(name='task4', active=True, schedule='0 2 * * *')

    dbsession.add(task1)
    dbsession.add(task2)
    dbsession.add(task3)
    dbsession.add(task4)
    dbsession.commit()

    return [task1, task2, task3, task4]

## TODO: test multiple works pulling, one wins

## TODO: test task inactive

## TODO: test touching / relocking?

## TODO: test passing task_names

def test_schedule_recurring_task(dbsession, tasks):
    taskflow = Taskflow()
    taskflow.add_tasks(tasks)
    scheduler = Scheduler(taskflow, now_override=datetime(2017, 6, 3, 6))
    scheduler.run(dbsession)

    task_instances = dbsession.query(TaskInstance).all()

    ## TODO: how was this commited before session.commit() was added queue_task ?
    ## TODO: ^ test a future run_at

    assert len(task_instances) == 2
    for task_instance in task_instances:
        assert task_instance.status == 'queued'
        assert task_instance.scheduled == True

        if task_instance.task_name == 'task2':
            assert task_instance.run_at == datetime(2017, 6, 4, 6)
        else:
            assert task_instance.run_at == datetime(2017, 6, 4, 2)

def test_queue_unique(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance1 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6), unique='foo')
    dbsession.add(task_instance1)
    dbsession.commit()

    with pytest.raises(IntegrityError):
        task_instance2 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 8), unique='foo')
        dbsession.add(task_instance2)
        dbsession.commit()

def test_queue_pull_task(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

def test_queue_pull_task_priority(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance1 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    task_instance2 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6), priority='high')
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.commit()

    normal_task_instance_id = task_instance1.id
    high_task_instance_id = task_instance2.id

    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', max_tasks=1, now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == high_task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'
    assert pulled_task_instance.priority == 'high'

    pulled_task_instances = taskflow.pull(dbsession, 'test', max_tasks=1, now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == normal_task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'
    assert pulled_task_instance.priority == 'normal'

def test_queue_pull_task_run_at_order(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance1 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6, 0, 5))
    task_instance2 = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6, 0, 10))
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.commit()

    sooner_task_instance_id = task_instance1.id
    later_task_instance_id = task_instance2.id

    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', max_tasks=1, now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == sooner_task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

    pulled_task_instances = taskflow.pull(dbsession, 'test', max_tasks=1, now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == later_task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

def test_succeed_task(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

    pulled_task_instance.succeed(dbsession, now=datetime(2017, 6, 4, 6, 0, 15))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'success'
    assert pulled_task_instance.ended_at == datetime(2017, 6, 4, 6, 0, 15)

def test_fail_task(dbsession, engine):
    task1 = Task(name='task1', active=True)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

    pulled_task_instance.fail(dbsession, now=datetime(2017, 6, 4, 6, 0, 15))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'failed'
    assert pulled_task_instance.ended_at == datetime(2017, 6, 4, 6, 0, 15)

def test_timeout_task(dbsession, engine):
    task1 = Task(name='task1', active=True, retries=2)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

    dbsession.expunge_all()

    ## pull 5 minutes later
    pulled_task_instances = taskflow.pull(dbsession, 'test2', now=datetime(2017, 6, 4, 6, 5, 15))
    pulled_task_instance = pulled_task_instances[0]
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 5, 15)
    assert pulled_task_instance.worker_id == 'test2'
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.attempts == 2

def test_custom_timeout_task(dbsession, engine):
    task1 = Task(name='task1', active=True, retries=2, timeout=7200)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 0, 12))
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.worker_id == 'test'

    dbsession.expunge_all()

    ## pull almost 2 hours later
    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 8, 0, 10))
    assert pulled_task_instances == []

    ## pull after 2 hours later
    pulled_task_instances = taskflow.pull(dbsession, 'test2', now=datetime(2017, 6, 4, 8, 0, 15))
    pulled_task_instance = pulled_task_instances[0]
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 8, 0, 15)
    assert pulled_task_instance.worker_id == 'test2'
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.attempts == 2

def test_task_retry_success(dbsession, engine):
    task1 = Task(name='task1', active=True, retries=1)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    now = datetime(2017, 6, 4, 6, 0, 12)

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=now)
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == now
    assert pulled_task_instance.started_at == now
    assert pulled_task_instance.worker_id == 'test'

    pulled_task_instance.fail(dbsession, now=datetime(2017, 6, 4, 6, 0, 15))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'retry'

    dbsession.expunge_all()

    ## stays within retry_delay
    pulled_task_instances = taskflow.pull(dbsession, 'test', now=datetime(2017, 6, 4, 6, 5, 14))
    assert pulled_task_instances == []

    pulled_task_instances = taskflow.pull(dbsession, 'test2', now=datetime(2017, 6, 4, 6, 5, 16))
    pulled_task_instance = pulled_task_instances[0]
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 5, 16)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.attempts == 2
    assert pulled_task_instance.worker_id == 'test2'

    pulled_task_instance.succeed(dbsession, now=datetime(2017, 6, 4, 6, 5, 20))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'success'
    assert pulled_task_instance.ended_at == datetime(2017, 6, 4, 6, 5, 20)

def test_task_retry_fail(dbsession, engine):
    task1 = Task(name='task1', active=True, retries=1)
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    task_instance_id = task_instance.id
    dbsession.expunge_all()

    now = datetime(2017, 6, 4, 6, 0, 12)

    pulled_task_instances = taskflow.pull(dbsession, 'test', now=now)
    pulled_task_instance = pulled_task_instances[0]

    assert pulled_task_instance.id == task_instance_id
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == now
    assert pulled_task_instance.started_at == now
    assert pulled_task_instance.worker_id == 'test'

    pulled_task_instance.fail(dbsession, now=datetime(2017, 6, 4, 6, 0, 15))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'retry'

    dbsession.expunge_all()

    pulled_task_instances = taskflow.pull(dbsession, 'test2', now=datetime(2017, 6, 4, 6, 5, 16))
    pulled_task_instance = pulled_task_instances[0]
    assert pulled_task_instance.status == 'running'
    assert pulled_task_instance.locked_at == datetime(2017, 6, 4, 6, 5, 16)
    assert pulled_task_instance.started_at == datetime(2017, 6, 4, 6, 0, 12)
    assert pulled_task_instance.attempts == 2
    assert pulled_task_instance.worker_id == 'test2'

    pulled_task_instance.fail(dbsession, now=datetime(2017, 6, 4, 6, 5, 20))
    dbsession.refresh(pulled_task_instance)

    assert pulled_task_instance.status == 'failed'
    assert pulled_task_instance.ended_at == datetime(2017, 6, 4, 6, 5, 20)
