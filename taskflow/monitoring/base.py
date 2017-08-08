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

    def heartbeat_scheduler(self, *args):
        self.call_destinations('heartbeat_scheduler', *args)

    def task_retry(self, *args):
        self.call_destinations('task_retry', *args)

    def task_failed(self, *args):
        self.call_destinations('task_failed', *args)

    def task_success(self, *args):
        self.call_destinations('task_success', *args)

    def workflow_failed(self, *args):
        self.call_destinations('workflow_failed', *args)

    def workflow_success(self, *args):
        self.call_destinations('workflow_success', *args)


class MonitorDestination(object):
    def heartbeat_scheduler(self, session):
        pass

    def task_retry(self, session, task_instance):
        pass

    def task_failed(self, session, task_instance):
        pass

    def task_success(self, session, task_instance):
        pass

    def workflow_failed(self, session, workflow_instance):
        pass

    def workflow_success(self, session, workflow_instance):
        pass
