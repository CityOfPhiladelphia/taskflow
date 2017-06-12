from datetime import datetime
from functools import reduce
import uuid

import pytest
import boto3

from taskflow import Scheduler, Pusher, Taskflow, Task, TaskInstance
from taskflow.push_workers.aws_batch import AWSBatchPushWorker
from shared_fixtures import *

get_logging()

class MockAWSBatch(object):
    def __init__(self, jobs, status):
        self.jobs = jobs
        self.status = status

    def describe_jobs(self, *args, **kwargs):
        jobs_with_status = self.jobs.copy()
        for job in jobs_with_status:
            job['status'] = self.status
        return {'jobs': jobs_with_status}

    def submit_job(self, *args, **job):
        job['jobId'] = str(uuid.uuid4())
        self.jobs.append(job)
        return {
            'jobName': job['jobName'],
            'jobId': job['jobId']
        }

def mockbatch(mock_aws_batch):
    def mockclient(aws_resource):
        if aws_resource != 'batch':
            raise Exception('Only AWS Batch is mocked!')
        return mock_aws_batch
    return mockclient

def test_push(dbsession, monkeypatch):
    mock_aws_batch = MockAWSBatch([], 'SUBMITTED')
    monkeypatch.setattr(boto3, 'client', mockbatch(mock_aws_batch))

    task1 = Task(name='task1', active=True, push_destination='aws_batch')
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)
    taskflow.add_push_worker(AWSBatchPushWorker(taskflow))
    taskflow.sync_db(dbsession)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    pusher = Pusher(taskflow, now_override=datetime(2017, 6, 4, 6))
    pusher.run(dbsession)

    task_instances = dbsession.query(TaskInstance).all()
    pushed_task_instance = task_instances[0]
    assert pushed_task_instance.status == 'pushed'
    assert 'jobId' in pushed_task_instance.push_state
    assert pushed_task_instance.push_state['jobName'] == 'task1__1'

    mock_aws_batch.status = 'PENDING'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'pushed'

    mock_aws_batch.status = 'RUNNABLE'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'pushed'

    mock_aws_batch.status = 'RUNNING'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'running'

    mock_aws_batch.status = 'STARTING'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'running'

    mock_aws_batch.status = 'SUCCEEDED'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'success'

def test_fail(dbsession, monkeypatch):
    mock_aws_batch = MockAWSBatch([], 'SUBMITTED')
    monkeypatch.setattr(boto3, 'client', mockbatch(mock_aws_batch))

    task1 = Task(name='task1', active=True, push_destination='aws_batch')
    dbsession.add(task1)
    taskflow = Taskflow()
    taskflow.add_task(task1)
    taskflow.add_push_worker(AWSBatchPushWorker(taskflow))
    taskflow.sync_db(dbsession)

    task_instance = task1.get_new_instance(run_at=datetime(2017, 6, 4, 6))
    dbsession.add(task_instance)
    dbsession.commit()

    pusher = Pusher(taskflow, now_override=datetime(2017, 6, 4, 6))
    pusher.run(dbsession)

    task_instances = dbsession.query(TaskInstance).all()
    pushed_task_instance = task_instances[0]
    assert pushed_task_instance.status == 'pushed'
    assert 'jobId' in pushed_task_instance.push_state
    assert pushed_task_instance.push_state['jobName'] == 'task1__1'

    mock_aws_batch.status = 'FAILED'
    pusher.run(dbsession)
    assert pushed_task_instance.status == 'failed'

## TODO: test task and task_instance job_queue param

## TODO: test task and task_instance job_definition param

## TODO: assert that all parameters are strings

## TODO: assert that all environment values are string

## TODO: assert env vars exists, each individually
