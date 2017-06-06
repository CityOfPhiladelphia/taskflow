from taskflow import Task

class BashTask(Task):
    def __init__(self, *args, **kwargs):
        kwargs['fn'] = self.execute

        super(BashTask, self).__init__(*args, **kwargs)

    def get_command(self):
        return self.params['command']

    def execute(self):
        pass
