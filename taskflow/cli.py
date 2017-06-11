import os
import time
import logging
from datetime import datetime
import json
import os
import socket
import sys

import requests
import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from taskflow import Scheduler, Pusher, Taskflow, Worker, TaskInstance
from taskflow.core.models import BaseModel

def get_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

def get_worker_id():
    worker_components = []

    ## AWS
    try:
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id', timeout=0.1)
        if response.status_code == 200:
            worker_components.append(response.text)
    except:
        pass

    ## ECS (AWS Batch uses ECS as well)
    try:
        response = requests.get('http://172.17.0.1:51678/v1/tasks', timeout=0.1)
        if response.status_code == 200:
            tasks = response.json()['Tasks']
            short_docker_id = os.getenv('HOSTNAME', None) ## ECS marks the short docker id as the HOSTNAME
            if short_docker_id != None:
                matched = list(filter(
                    lambda ecs_task: ecs_task['Containers'][0]['DockerId'][0:12] == short_docker_id,
                    tasks))
                if len(matched) > 0:
                    worker_components.append(matched[0]['Containers'][0]['Arn'])
    except:
        pass

    ## fallback to IP
    if len(worker_components) == 0:
        return socket.gethostbyname(socket.gethostname())
    else:
        return '-'.join(worker_components)

@click.group()
@click.option('--taskflow')
@click.pass_context
def main(ctx, taskflow):
    if taskflow != None:
        ## TODO: load taskflow from option using dynamic import
        ctx.obj['taskflow'] = taskflow

@main.command()
@click.pass_context
def api_server(ctx):
    pass

@main.command()
@click.option('--sql-alchemy-connection')
@click.pass_context
def init_db(ctx, sql_alchemy_connection):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    BaseModel.metadata.create_all(engine)

@main.command()
@click.option('--sql-alchemy-connection')
@click.option('-n','--num-runs', type=int, default=10)
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--now-override')
@click.option('--sleep', type=int, default=5)
@click.pass_context
def scheduler(ctx, sql_alchemy_connection, num_runs, dry_run, now_override, sleep):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    session = Session()
    taskflow = ctx.obj['taskflow']
    taskflow.sync_db(session)
    session.close()

    if now_override != None:
        now_override = datetime.strptime(now_override, '%Y-%m-%dT%H:%M:%S')

    scheduler = Scheduler(taskflow, dry_run=dry_run, now_override=now_override)
    pusher = Pusher(taskflow, dry_run=dry_run, now_override=now_override)

    for n in range(0, num_runs):
        session = Session()
        taskflow.sync_db(session)

        scheduler.run(session)
        pusher.run(session)

        session.close()
        time.sleep(sleep)

@main.command()
@click.option('--sql-alchemy-connection')
@click.option('-n','--num-runs', type=int, default=10)
@click.option('--dry-run', is_flag=True, default=False)
@click.option('--now-override')
@click.option('--sleep', type=int, default=5)
@click.option('--task-names')
@click.option('--worker-id')
@click.pass_context
def pull_worker(ctx, sql_alchemy_connection, num_runs, dry_run, now_override, sleep, task_names, worker_id):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    session = Session()
    taskflow = ctx.obj['taskflow']
    taskflow.sync_db(session)
    session.close()

    worker = Worker(taskflow)

    if now_override != None:
        now_override = datetime.strptime(now_override, '%Y-%m-%dT%H:%M:%S')

    if task_names != None:
        task_names = task_names.split(',')

    if worker_id == None:
        worker_id = get_worker_id()

    for n in range(0, num_runs):
        session = Session()
        
        task_instances = taskflow.pull(session, worker_id, task_names=task_names, now=now_override)
        
        if len(task_instances) > 0:
            worker.execute(session, task_instances[0])

        session.close()

        if sleep > 0:
            time.sleep(sleep)

@main.command()
@click.argument('task_instance_id', type=int)
@click.option('--sql-alchemy-connection')
@click.option('--worker-id')
@click.pass_context
def run_task(ctx, task_instance_id, sql_alchemy_connection, worker_id):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    if worker_id == None:
        worker_id = get_worker_id()

    session = Session()

    taskflow = ctx.obj['taskflow']
    taskflow.sync_db(session)

    task_instance = session.query(TaskInstance).get(task_instance_id)

    worker = Worker(taskflow)
    success = worker.execute(session, task_instance)

    session.close()

    if not success:
        sys.exit(1)

@main.command()
@click.argument('task_name')
@click.option('--workflow-instance-id')
@click.option('--run-at')
@click.option('--priority')
@click.option('--params')
@click.option('--sql-alchemy-connection')
@click.pass_context
def queue_task(ctx, task_name, workflow_instance_id, run_at, priority, params, sql_alchemy_connection):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    session = Session()

    taskflow = ctx.obj['taskflow']
    taskflow.sync_db(session)

    task = taskflow.get_task(task_name)

    if task == None:
        raise Exception('Task `{}` not found'.format(task_name))

    if params != None:
        params = json.loads(params)

    task_instance = task.get_new_instance(
        run_at=run_at,
        workflow_instance_id=workflow_instance_id,
        priority=priority,
        params=params)

    session.add(task_instance)
    session.commit()
    session.close()

@main.command()
@click.argument('workflow_name')
@click.option('--run-at')
@click.option('--priority')
@click.option('--sql-alchemy-connection')
@click.pass_context
def queue_workflow(ctx, workflow_name, run_at, priority, sql_alchemy_connection):
    connection_string = sql_alchemy_connection or os.getenv('SQL_ALCHEMY_CONNECTION')
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)

    session = Session()

    taskflow = ctx.obj['taskflow']
    taskflow.sync_db(session)

    workflow = taskflow.get_workflow(workflow_name)

    if workflow == None:
        raise Exception('Workflow `{}` not found'.format(workflow_name))

    workflow_instance = workflow.get_new_instance(
        run_at=run_at,
        priority=priority)

    session.add(workflow_instance)
    session.commit()
    session.close()

def cli(taskflow):
    return main(obj={'taskflow': taskflow})

if __name__ == '__main__':
    get_logging()
    main(obj={})
