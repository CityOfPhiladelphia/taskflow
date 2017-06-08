import logging
import signal

from .models import Workflow, WorkflowInstance, Task, TaskInstance

class Worker(object):
    def __init__(self, taskflow):
        self.logger = logging.getLogger('Worker')
        self.taskflow = taskflow

        signal.signal(signal.SIGINT, self.on_kill)
        signal.signal(signal.SIGTERM, self.on_kill)

    def on_kill(self):
        if hasattr(self, task):
            self.task.on_kill()

    def execute(self, session, task_instance):
        try:
            task = self.taskflow.get_task(task_instance.task_name)
            self.current_task = task
            task.execute(task_instance)
        except Exception:
            self.logger.exception('Error executing: %s %s', task_instance.task_name, task_instance.id)
            task_instance.fail(session) ## TODO: retry?
        task_instance.succeed(session)
