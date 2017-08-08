import re
import json

from cron_descriptor import get_description
import requests

from taskflow import WorkflowInstance, TaskInstance
from .base import MonitorDestination

class SlackMonitor(MonitorDestination):
    def __init__(self, session, taskflow, slack_url=None):
        self.slack_url = slack_url or os.getenv('SLACK_WEBHOOK_URL')
        self.session = session
        self.taskflow = taskflow

    def get_log_url(self, failed_task):
        push_destination = self.taskflow.get_task(failed_task.task_name).push_destination
        push_worker = self.taskflow.get_push_worker(push_destination)
        if push_worker and hasattr(push_worker, 'get_log_url'):
            return push_worker.get_log_url(failed_task)
        return None

    def get_message(self, item):
        if isinstance(item, WorkflowInstance):
            item_type = 'Workflow'
            name = item.workflow_name

            failed_tasks = self.session.query(TaskInstance)\
                .filter(TaskInstance.workflow_instance_id == item.id,
                        TaskInstance.status == 'failed')\
                .all()

            failed_task_attachments = []
            for failed_task in failed_tasks:
                failed_task_attachments.append({
                    'title': 'Failed Workflow Task',
                    'color': '#ff0000',
                    'fields': [
                        {
                            'title': 'Task',
                            'value': failed_task.task_name
                        },
                        {
                            'title': 'ID',
                            'value': failed_task.id
                        },
                        {
                            'title': 'Number of Attempts',
                            'value': failed_task.attempts
                        },
                        {
                            'title': 'Logs',
                            'value': self.get_log_url(failed_task)
                        }
                    ]
                })
        else:
            item_type = 'Task'
            name = item.task_name

        attachments = [{
            'title': '{} Failure'.format(item_type),
            'text': '<!channel> A {} in Taskflow failed'.format(item_type.lower()),
            'color': '#ff0000',
            'fields': [
                {
                    'title': item_type,
                    'value': name,
                    'short': False
                },
                {
                    'title': 'ID',
                    'value': item.id
                },
                {
                    'title': 'Priority',
                    'value': item.priority
                },
                {
                    'title': 'Scheduled Run Time',
                    'value': '{:%Y-%m-%d %H:%M:%S}'.format(item.run_at)
                },
                {
                    'title': 'Start Time',
                    'value': '{:%Y-%m-%d %H:%M:%S}'.format(item.started_at)
                },
                {
                    'title': 'Failure Time',
                    'value': '{:%Y-%m-%d %H:%M:%S}'.format(item.ended_at)
                }
            ]
        }]

        if item.scheduled:
            workflow = self.taskflow.get_workflow(item.workflow_name)
            attachments[0]['fields'].append({
                'title': 'Schedule',
                'value': '{} ({})'.format(get_description(workflow.schedule), workflow.schedule)
            })

        if failed_task_attachments:
            attachments += failed_task_attachments
        else:
            attachments[0]['fields'].append({
                'title': 'Number of Attempts',
                'value': item.attempts
            })
            attachments[0]['fields'].append({
                'title': 'Logs',
                'value': self.get_log_url(item)
            })

        return {'attachments': attachments}

    def send_to_slack(self, message):
        requests.post(
            self.slack_url,
            data=json.dumps(message),
            headers={'Content-Type': 'application/json'})

    def heartbeat_scheduler(self):
        pass

    def task_retry(self, task_instance):
        pass

    def task_failed(self, task_instance):
        ## only alert on tasks not associated with a workflow.
        ## Task failures will bubble up to the workflow
        if task_instance.workflow_instance_id == None:
            message = self.get_message(task_instance)
            self.send_to_slack(message)

    def task_success(self, task_instance):
        pass

    def workflow_failed(self, workflow_instance):
        message = self.get_message(workflow_instance)
        self.send_to_slack(message)

    def workflow_success(self, workflow_instance):
        pass
