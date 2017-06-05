
class PushWorker(object):
    supports_state_sync = False
    push_type = None

    def __init__(self, taskflow, session):
        self.taskflow = taskflow
        self.session = session

    def sync_task_instance_states(self, task_instances):
        raise NotImplementedError()

    def push_task_instances(self, task_instances):
        raise NotImplementedError()
