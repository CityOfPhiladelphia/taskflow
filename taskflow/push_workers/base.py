
class PushWorker(object):
    supports_state_sync = False
    push_type = None

    def __init__(self, taskflow):
        self.taskflow = taskflow

    def sync_task_instance_states(self, session, task_instances):
        raise NotImplementedError()

    def push_task_instances(self, session, task_instances):
        raise NotImplementedError()
