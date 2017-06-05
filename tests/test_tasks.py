from datetime import datetime

import pytest

from taskflow import Scheduler, Taskflow, Task, TaskInstance
from shared_fixtures import *

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

def test_schedule_recurring_task(dbsession, tasks):
    taskflow = Taskflow()
    taskflow.add_tasks(tasks)
    print(len(taskflow._workflows))
    scheduler = Scheduler(dbsession, taskflow, now_override=datetime(2017, 6, 3, 6))
    scheduler.run()

    task_instances = dbsession.query(TaskInstance).all()

    assert len(task_instances) == 2
    for task_instance in task_instances:
        assert task_instance.status == 'queued'
        assert task_instance.scheduled == True

        if task_instance.task_name == 'task2':
            assert task_instance.run_at == datetime(2017, 6, 4, 6)
        else:
            assert task_instance.run_at == datetime(2017, 6, 4, 2)

## TODO: queue a task with unique string

## TODO: test unique conflict

## TODO: test pulling a task

## TODO: test pulling a task priority

## TODO: test touching / relocking?

## TODO: test retry and succeed

## TODO: test retry and fail

## TODO: test succeeding a task

## TODO: test failing a task
