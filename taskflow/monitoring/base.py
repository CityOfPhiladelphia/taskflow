
class Monitor(object):
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
