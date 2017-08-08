from datetime import datetime

import requests_mock
from restful_ben.test_utils import dict_contains

from taskflow import WorkflowInstance, TaskInstance
from taskflow.monitoring.slack import SlackMonitor

from shared_fixtures import *

def test_slack_monitor(dbsession, workflows):
    taskflow = Taskflow()
    taskflow.add_workflows(workflows)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 3, 6),
        started_at=datetime(2017, 6, 3, 6),
        ended_at=datetime(2017, 6, 3, 6, 0, 36),
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

    slack_url = 'http://fakeurl.com/foo'

    slack_monitor = SlackMonitor(taskflow, slack_url=slack_url)

    with requests_mock.Mocker() as m:
        m.post(slack_url)

        slack_monitor.workflow_failed(dbsession, workflow_instance)

        assert m.called
        assert m.call_count == 1

        request = m.request_history[0]
        data = request.json()
        assert dict_contains(data['attachments'][0], {
            'text': '<!channel> A workflow in Taskflow failed',
            'title': 'Workflow Failure',
            'color': '#ff0000',
            'fields': [
                {
                    'title': 'Workflow',
                    'short': False,
                    'value': 'workflow1'
                },
                {
                    'title': 'ID',
                    'value': 1
                },
                {
                    'title': 'Priority',
                    'value': 'normal'
                },
                {
                    'title': 'Scheduled Run Time',
                    'value': '2017-06-03 06:00:00'
                },
                {
                    'title': 'Start Time',
                    'value': '2017-06-03 06:00:00'
                },
                {
                    'title': 'Failure Time',
                    'value': '2017-06-03 06:00:36'
                },
                {
                    'title': 'Schedule',
                    'value': 'At 06:00 AM (0 6 * * *)'
                }
            ]
        })
        assert dict_contains(data['attachments'][1], {
            'title': 'Failed Workflow Task',
            'color': '#ff0000',
            'fields': [
                {
                    'title': 'Task',
                    'value': 'task3'
                },
                {
                    'title': 'ID',
                    'value': 3
                },
                {
                    'title': 'Number of Attempts',
                    'value': 1
                },
                {
                    'title': 'Logs',
                    'value': None
                }
            ]
        })