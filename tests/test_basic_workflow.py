from datetime import datetime

import pytest

from taskflow import Scheduler, Taskflow, Workflow, WorkflowInstance, Task, TaskInstance
from shared_fixtures import *

get_logging()

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

## TODO: test dry run

def test_schedule_recurring_workflow(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)
    scheduler = Scheduler(taskflow, now_override=datetime(2017, 6, 3, 6))
    scheduler.run(dbsession)

    workflow_instances = dbsession.query(WorkflowInstance).all()
    assert len(workflow_instances) == 1
    assert workflow_instances[0].status == 'queued'
    assert workflow_instances[0].scheduled == True
    assert workflow_instances[0].run_at == datetime(2017, 6, 4, 6)

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 0

def test_workflow_starts(dbsession, workflows):
    now = datetime(2017, 6, 3, 6, 12)

    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        status='queued',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()

    scheduler = Scheduler(taskflow, now_override=now)
    scheduler.run(dbsession)

    assert workflow_instance.status == 'running'
    assert workflow_instance.started_at == now

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 2
    for instance in task_instances:
        assert instance.task_name in ['task1','task2']
        assert instance.status == 'queued'

def test_schedule_recurring_workflow(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        status='queued',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()

    scheduler = Scheduler(taskflow, now_override=datetime(2017, 6, 3, 6, 0, 45))
    scheduler.run(dbsession)

    dbsession.refresh(workflow_instance)
    assert workflow_instance.status == 'running'

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 2
    for instance in task_instances:
        assert instance.task_name in ['task1','task2']
        assert instance.status == 'queued'

def test_workflow_running_no_change(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow1 = workflows[0]

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
        status='running',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance2 = TaskInstance(
        task_name='task2',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='running',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.commit()

    scheduler = Scheduler(taskflow, now_override=datetime(2017, 6, 3, 6, 12))
    scheduler.run(dbsession)

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 2
    for instance in task_instances:
        assert instance.task_name in ['task1','task2']
        assert instance.status == 'running'

def test_workflow_next_step(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        status='running',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()
    task_instance1 = TaskInstance(
        task_name='task1',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
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
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.commit()

    scheduler = Scheduler(taskflow, now_override=datetime(2017, 6, 3, 6, 12))
    scheduler.run(dbsession)

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 3
    for instance in task_instances:
        assert instance.task_name in ['task1','task2','task3']
        if instance.task_name in ['task1','task2']:
            assert instance.status == 'success'
        elif instance.task_name == 'task3':
            assert instance.status == 'queued'

def test_workflow_success(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        status='running',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()
    task_instance1 = TaskInstance(
        task_name='task1',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
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
        run_at=datetime(2017, 6, 3, 6, 0, 34),
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
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance4 = TaskInstance(
        task_name='task4',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
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

    now = datetime(2017, 6, 3, 6, 12)

    scheduler = Scheduler(taskflow, now_override=now)
    scheduler.run(dbsession)

    dbsession.refresh(workflow_instance)
    assert workflow_instance.status == 'success'
    assert workflow_instance.ended_at == now

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 4
    for instance in task_instances:
        assert instance.task_name in ['task1','task2','task3','task4']
        assert instance.status == 'success'

def test_workflow_fail(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        status='running',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()
    task_instance1 = TaskInstance(
        task_name='task1',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='success',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
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
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    task_instance3 = TaskInstance(
        task_name='task3',
        scheduled=True,
        workflow_instance_id=workflow_instance.id,
        status='failed',
        run_at=datetime(2017, 6, 3, 6, 0, 34),
        attempts=1,
        priority='normal',
        push=False,
        timeout=300,
        retry_delay=300)
    dbsession.add(task_instance1)
    dbsession.add(task_instance2)
    dbsession.add(task_instance3)
    dbsession.commit()

    now = datetime(2017, 6, 3, 6, 12)

    scheduler = Scheduler(taskflow, now_override=now)
    scheduler.run(dbsession)

    dbsession.refresh(workflow_instance)
    assert workflow_instance.status == 'failed'
    assert workflow_instance.ended_at == now

    task_instances = dbsession.query(TaskInstance).all()
    assert len(task_instances) == 3
    for instance in task_instances:
        assert instance.task_name in ['task1','task2','task3']
        if instance.task_name in ['task1','task2']:
            assert instance.status == 'success'
        elif instance.task_name == 'task3':
            assert instance.status == 'failed'
