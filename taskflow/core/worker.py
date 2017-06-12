import logging
import signal
import sys

from .models import Workflow, WorkflowInstance, Task, TaskInstance

class Worker(object):
    def __init__(self, taskflow):
        self.logger = logging.getLogger('Worker')
        self.taskflow = taskflow

        signal.signal(signal.SIGINT, self.on_kill)
        signal.signal(signal.SIGTERM, self.on_kill)

    def on_kill(self, sig_num, stack_frame):
        if hasattr(self, 'task'):
            self.task.on_kill()
        sys.exit(0)

    def execute(self, session, task_instance):
        try:
            task = self.taskflow.get_task(task_instance.task_name)
            if not task:
                raise Exception('Task `{}` does not exist'.format(task_instance.task_name))

            self.current_task = task
            task.execute(task_instance)
        except Exception:
            self.logger.exception('Error executing: %s %s', task_instance.task_name, task_instance.id)
            task_instance.fail(session) ## TODO: retry?
            return False
        task_instance.succeed(session)
        return True
