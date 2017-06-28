import os
import time
import logging
from datetime import datetime
import json

from taskflow import cli, Taskflow

from taskflow_basic_example.workflows import workflows
from taskflow_basic_example.tasks import tasks

DEBUG = os.getenv('DEBUG', 'False') == 'True'

def get_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if DEBUG:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger.setLevel(level)

def get_taskflow():
    taskflow = Taskflow()

    taskflow.add_workflows(workflows)
    taskflow.add_tasks(tasks)

    return taskflow

if __name__ == '__main__':
    get_logging()
    taskflow = get_taskflow()
    cli(taskflow)
