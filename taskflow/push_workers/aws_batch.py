import logging

import boto3

from .base import PushWorker

class AWSBatchPushWorker(PushWorker):
    supports_state_sync = True
    push_type = 'aws_batch'

    def __init__(self, *args, default_job_queue=None, default_job_definition=None, **kwargs):
        self.logger = logging.getLogger('AWSBatchPushWorker')

        super(AWSBatchPushWorker, self).__init__(*args, **kwargs)

        self.batch_client = boto3.client('batch')
        self.default_job_queue = default_job_queue
        self.default_job_definition = default_job_definition

    def sync_task_instance_states(self, session, dry_run, task_instances):
        jobs = dict()
        for task_instance in task_instances:
            jobs[task_instance.push_state['jobId']] = task_instance

        response = self.batch_client.describe_jobs(jobs=list(jobs.keys())) ## TODO: batch by 100

        ## TODO: timeout pushed tasks?
        ## TODO: tasks that are 'pushed' but not in AWS batch?

        for job in response['jobs']:
            if job['status'] in ['SUBMITTED','PENDING','RUNNABLE']:
                status = 'pushed'
            elif job['status'] in ['STARTING','RUNNING']:
                status = 'running'
            elif job['status'] == 'SUCCEEDED':
                status = 'success'
            elif job['status'] == 'FAILED':
                status = 'failed'

            task_instance = jobs[job['jobId']]
            if task_instance.status != status:
                task_instance.status = status

        if not dry_run:
            session.commit()

    def get_job_name(self, workflow, task, task_instance):
        if workflow != None:
            return '{}__{}__{}__{}'.format(
                workflow.name,
                task_instance.workflow_instance_id,
                task.name,
                task_instance.id)
        else:
            return '{}__{}'.format(
                task.name,
                task_instance.id)

    def push_task_instances(self, session, dry_run, task_instances):
        for task_instance in task_instances:
            try:
                task = self.taskflow.get_task(task_instance.task_name)
                workflow = None

                if task == None:
                    workflow = self.taskflow.get_workflow(task.workflow)
                    task = workflow.get_task(task_instance.task_name)

                if task == None:
                    raise Exception('Task `{}` not found'.format(task_instance.task_name))

                parameters = {
                    'task': task.name,
                    'task_instance': str(task_instance.id)
                }

                if workflow != None:
                    parameters['workflow'] = workflow.name
                    parameters['workflow_instance'] = str(task_instance.workflow_instance_id)

                if 'job_queue' in task_instance.params and task_instance.params['job_queue']:
                    job_queue = task_instance.params['job_queue']
                elif 'job_queue' in task.params and task.params['job_queue']:
                    job_queue = task.params['job_queue']
                else:
                    job_queue = self.default_job_queue

                if 'job_definition' in task_instance.params and task_instance.params['job_definition']:
                    job_definition = task_instance.params['job_definition']
                elif 'job_definition' in task.params and task.params['job_definition']:
                    job_definition = task.params['job_definition']
                else:
                    job_definition = self.default_job_definition

                environment = [
                    {
                        'name': 'TASKFLOW_TASK',
                        'value': task.name
                    },
                    {
                        'name': 'TASKFLOW_TASK_INSTANCE_ID',
                        'value': str(task_instance.id)
                    }
                ]

                if workflow != None:
                    environment.append({
                        'name': 'TASKFLOW_WORKFLOW',
                        'value': workflow.name
                    })
                    environment.append({
                        'name': 'TASKFLOW_WORKFLOW_INSTANCE_ID',
                        'value': str(task_instance.workflow_instance_id)
                    })

                job_name = self.get_job_name(workflow, task, task_instance)

                self.logger.info('Submitting job: %s %s %s', job_name, job_queue, job_definition)

                if not dry_run:
                    response = self.batch_client.submit_job(
                        jobName=job_name,
                        jobQueue=job_queue,
                        jobDefinition=job_definition,
                        parameters=parameters,
                        containerOverrides={
                            'environment': environment
                        })

                task_instance.state = 'pushed'
                task_instance.push_state = response

                if not dry_run:
                    session.commit()
            except Exception:
                ## TODO: rollback?
                self.logger.exception('Exception submitting %s %s', task_instance.task_name, task_instance.id)
