from datetime import datetime
from itertools import groupby

from .models import Workflow, WorkflowInstance, TaskInstance

class Pusher(object):
    def __init__(self, session, taskflow, now_override=None):
        self.session = session
        self.taskflow = taskflow

        self.now_override = now_override

    def now(self):
        """Allows for dry runs and tests to use a specific datetime as now"""
        if self.now_override:
            return self.now_override
        return datetime.utcnow()

    def get_push_destination(self, task_instance):
        return self.taskflow.get_task(task_instance.task_name).push_destination

    def sync_task_states(self):
        ## TODO: paginate?
        task_instances = self.session.query(TaskInstance)\
            .filter(TaskInstance.push == True, TaskInstance.status.in_(['pushed','running','retrying'])).all()

        for push_destination, task_instances in groupby(task_instances, self.get_push_destination):
            push_worker = self.taskflow.get_push_worker(push_destination)
            push_worker.sync_task_instance_states(task_instances)

    def push_queued_task_instances(self):
        task_instances = self.taskflow.pull('Pusher', max_tasks=100, now=self.now(), push=True)

        for push_destination, task_instances in groupby(task_instances, self.get_push_destination):
            push_worker = self.taskflow.get_push_worker(push_destination)
            push_worker.push_task_instances(task_instances)

    def run(self):
        self.push_queued_task_instances()
        self.sync_task_states()
