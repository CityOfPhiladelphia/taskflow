from taskflow import Workflow
from taskflow.tasks.bash_task import BashTask

echo_workflow = Workflow(
    name='echo_workflow',
    active=True,
    schedule='0 * * * *')

echo_a = BashTask(workflow=echo_workflow,
                  name='echo_a',
                  active=True,
                  retries=2, # this task is allowed to retry
                  params={
                      'command': 'echo "Foo A"'
                  })

echo_b = BashTask(workflow=echo_workflow,
                  name='echo_b',
                  active=True,
                  retries=2, # this task is allowed to retry
                  retry_delay=1200, # this task can be expensive, lets wait 20 minutes between retries
                  params={
                      'command': 'echo "Foo B"'
                  })

echo_c = BashTask(workflow=echo_workflow,
                  name='echo_c',
                  active=True,
                  params={
                      'command': 'echo "Foo C"'
                  })

echo_d = BashTask(workflow=echo_workflow,
                  name='echo_d',
                  active=True,
                  params={
                      'command': 'echo "Foo D"'
                  })

echo_c.depends_on(echo_a)
echo_c.depends_on(echo_b)
echo_d.depends_on(echo_c)

## A and B should run in parallel, while C waits, D should then wait for C
