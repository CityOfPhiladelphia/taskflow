import logging

from .models import Workflow, WorkflowInstance, Task, TaskInstance

class Worker(object):
    def __init__(self, taskflow):
        self.logger = logging.getLogger('Worker')
        self.taskflow = taskflow

    def execute(self, session, task_instance):
        try:
            task = self.taskflow.get_task(task_instance.task_name)
            task.execute(task_instance)
        except Exception:
            self.logger.exception('Error executing: %s %s', task_instance.task_name, task_instance.id)
            task_instance.fail(session) ## TODO: retry?
        task_instance.succeed(session)
