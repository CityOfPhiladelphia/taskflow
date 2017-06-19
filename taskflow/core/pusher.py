from datetime import datetime
from itertools import groupby
import logging

from .models import Workflow, WorkflowInstance, TaskInstance

class Pusher(object):
    def __init__(self, taskflow, dry_run=None, now_override=None):
        self.logger = logging.getLogger('Pusher')

        self.taskflow = taskflow

        self.dry_run = dry_run
        self.now_override = now_override

    def now(self):
        """Allows for dry runs and tests to use a specific datetime as now"""
        if self.now_override:
            return self.now_override
        return datetime.utcnow()

    def get_push_destination(self, task_instance):
        return self.taskflow.get_task(task_instance.task_name).push_destination

    def sync_task_states(self, session):
        ## TODO: paginate?
        task_instances = session.query(TaskInstance)\
            .filter(TaskInstance.push == True, TaskInstance.status.in_(['pushed','running'])).all()

        for push_destination, task_instances in groupby(task_instances, self.get_push_destination):
            self.logger.info('Syncing states with %s', push_destination)

            try:
                push_worker = self.taskflow.get_push_worker(push_destination)
                push_worker.sync_task_instance_states(session, self.dry_run, task_instances)
            except Exception:
                ## TODO: rollback?
                self.logger.exception('Exception syncing with %s', push_destination)


    def push_queued_task_instances(self, session):
        task_instances = self.taskflow.pull(session, 'Pusher', max_tasks=100, now=self.now(), push=True)

        for push_destination, task_instances in groupby(task_instances, self.get_push_destination):
            self.logger.info('Pushing to %s', push_destination)

            try:
                push_worker = self.taskflow.get_push_worker(push_destination)
                push_worker.push_task_instances(session, self.dry_run, task_instances)
            except Exception:
                ## TODO: rollback?
                self.logger.exception('Exception pushing to %s', push_destination)

    def run(self, session):
        self.logger.info('*** Starting Pusher Run ***')

        self.logger.info('Pushing queued task instances')
        self.push_queued_task_instances(session)

        self.logger.info('Syncing pushed task instance states')
        self.sync_task_states(session)

        self.logger.info('*** End Pusher Run ***')
