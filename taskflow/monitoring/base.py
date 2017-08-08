import logging

class Monitor(object):
    def __init__(self, destinations=None):
        self.logger = logging.getLogger('Monitoring')

        self.destinations = destinations or []

    def call_destinations(self, fn_name, *args):
        for destination in self.destinations:
            try:
                getattr(destination, fn_name)(*args)
            except:
                self.logger.exception('Exception trying to call `{}` on the `{}` monitor.'\
                                      .format(fn_name, destination.__class__.__name__))

    def heartbeat_scheduler(self):
        self.call_destinations('heartbeat_scheduler')

    def task_retry(self, task_instance):
        self.call_destinations('task_retry', task_instance)

    def task_failed(self, task_instance):
        self.call_destinations('task_failed', task_instance)

    def task_success(self, task_instance):
        self.call_destinations('task_success', task_instance)

    def workflow_failed(self, workflow_instance):
        self.call_destinations('workflow_failed', workflow_instance)

    def workflow_success(self, workflow_instance):
        self.call_destinations('workflow_success', workflow_instance)


class MonitorDestination(object):
    def heartbeat_scheduler(self):
        pass

    def task_retry(self, task_instance):
        pass

    def task_failed(self, task_instance):
        pass

    def task_success(self, task_instance):
        pass

    def workflow_failed(self, workflow_instance):
        pass

    def workflow_success(self, workflow_instance):
        pass
