import boto3

from .base import MonitorDestination

class AWSMonitor(MonitorDestination):
    def __init__(self,
                 metric_prefix='',
                 metric_namespace='taskflow',
                 *args, **kwargs):
        self.metric_namespace = metric_namespace
        self.metric_prefix = metric_prefix
        self.cloudwatch = boto3.client('cloudwatch')

        super(AWSMonitor, self).__init__(*args, **kwargs)

    def heartbeat_scheduler(self, session):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'scheduler_heartbeat',
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])

    def task_retry(self, session, task_instance):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'task_retry',
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': self.metric_prefix + 'task_retry',
                    'Dimensions': [
                        {
                            'Name': 'task_name',
                            'Value': task_instance.task_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])

    def task_failed(self, session, task_instance):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'task_failure',
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': self.metric_prefix + 'task_failure',
                    'Dimensions': [
                        {
                            'Name': 'task_name',
                            'Value': task_instance.task_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])

    def task_success(self, session, task_instance):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'task_success',
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': self.metric_prefix + 'task_success',
                    'Dimensions': [
                        {
                            'Name': 'task_name',
                            'Value': task_instance.task_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])

    def workflow_failed(self, session, workflow_instance):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'workflow_failure',
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': self.metric_prefix + 'workflow_failure',
                    'Dimensions': [
                        {
                            'Name': 'workflow_name',
                            'Value': workflow_instance.workflow_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])

    def workflow_success(self, session, workflow_instance):
        self.cloudwatch.put_metric_data(
            Namespace=self.metric_namespace,
            MetricData=[
                {
                    'MetricName': self.metric_prefix + 'workflow_success',
                    'Value': 1,
                    'Unit': 'Count'
                },
                {
                    'MetricName': self.metric_prefix + 'workflow_success',
                    'Dimensions': [
                        {
                            'Name': 'workflow_name',
                            'Value': workflow_instance.workflow_name
                        }
                    ],
                    'Value': 1,
                    'Unit': 'Count'
                }
            ])
