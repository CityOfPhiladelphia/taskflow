# Taskflow

An advanced yet simple system to run your background tasks and workflows.

Features

- Recurring tasks (aka jobs) and workflows (a series of dependent tasks) with CRON-like scheduling


- Workflow dependencies - tasks execute in order and/or in parallel depending on dependency chain.

- Two types of workers that execute tasks
	- Pull workers - Pull tasks directly off the database queue and execute them.
	- Push workers - Tasks are pushed to a remote work management system, such as AWS Batch, Kubernetes, or Apache Mesos. Container friendly.

## Motivation

Other background task and workflow management solutions exist out there, such as Celery for tasks/jobs or Airflow and Luigi for workflows. Taskflow is designed to have a small footprint, maintain it's state in a readily queryable SQL database, have predictable CRON-like scheduling behavior, and GTFO of the way when you need it to.

## Concepts

### Task

A Task represents some runnable code or command. For example, extact a table from a database, or push to an API. Anything that can run in pyhon or be excuted in Bash.

### Task Instances

A Task Instance is a specific run of a Task. Task instances can be created programmatically on-demand or automatically using a recurring schedule attached to a Task.

#### Workflows

A Workflow represents a series of dependent tasks represent in a graph. Workflows are associated to the Tasks they run.

#### Workflow Instances

A Workflow Instance is a specific run of a Workflow. A Workflow Instance creates Task Instances as needed during a run. Like a Task Instance, Workflow Instances can be created programmatically on-demand or automatically using a recurring schedule attached to a Workflow.

#### Scheduler

The scheduler always runs as a single instance at a time. It schedules recurring Tasks and recurring Workflows. It also advances running Workflow Instances, scheduling Task Instances as needed.

#### Pusher

The Pusher is usually run within the same process as the scheduler. The Pusher pulls tasks destined for a push worker off the task_instances table and pushes them to the push destination. For examples, pushing tasks to AWS Batch. The Push also syncs the state of the currently pushed tasks with the push destination. Multiple push destinations can be used at the same time, for example one task could go to AWS Batch while another goes to Kubernetes.

#### Pull Worker

A pull worker is a process that directly pulls tasks off the queue and executes them.

## API

### Task

Creates a new Task definiton that can be used to schedule tasks and create task instances.

```python
task_foo = Task(
	name=None, # Required - name of the task, like 'push_new_accounts'
	workflow=None, # workflow the task is associated with
	active=False, # wether the task is active, if false, it will not be sceduled
	title=None, # human friendly title of the task, used for the UI
	description=None, # human friendly description of the task, used for the UI
	concurrency=1, # maximum number of TaskInstances of this Task that can be running at a time
	sla=None, # numbers of seconds the scheduled Task should start within
	schedule=None, # CRON pattern to schedule the Task, if the task is to be scheduled
	default_priority='normal', # default priority used for the TaskInstances of this Task
	start_date=None, # start datetime for scheduling this Task, the task will not be scheduled before this time
	end_date=None, # end datetime for scheduling this Task, the task will not be scheduled after this time
	retries=0, # number of times to retry task if it fails
	timeout=300, # if the task is not completed and the locked_at datetime is past these number of seconds, retry or fail task
	retry_delay=300, # number of seconds inbetween retries
	params={}, # parameters of the task, these are available to the executors and pushers
	push_destination=None) # for Tasks that need to be pushed, where to push them, for example 'aws_batch'
	
task_foo.depends_on(task_bar) # Task.depends_on tells Taskflow that one Task is dependant on another. Both tasks need to be in the same Workflow
```

To create a task with executable python code, override the `execute` function on an inherited class.

```python
class MyTask(Task):
	def execute(self, task_instance):
		print('Any sort of python I want')
```

### Workflow

Creates a new Workflow definiton that can be used to schedule workflows and create workflow instances.

```python
workflow_foo = Workflow(
	name=None, # Required. Name of the workflow like 'salesforce_etl'
	active=False, # wether the workflow is active, if false, it will not be sceduled
	title=None, # human friendly title of the workflow, used for the UI
	description=None, # human friendly description of the workflow, used for the UI
	concurrency=1, # maximum number of WorkflowInstances of this Workflow that can be running at a time
	sla=None, # numbers of seconds the scheduled Workflow should start within
	schedule=None, # CRON pattern to schedule the Workflow, if the workflow is to be scheduled
	default_priority='normal', # default priority used for the WorkflowInstances of this Workflow
	start_date=None, # start datetime for scheduling this Workflow, the workflow will not be scheduled before this time
	end_date=None, # end datetime for scheduling this Workflow, the workflow will not be scheduled after this time)
```

### Taskflow

The Taskflow class is used to create an instance of Taskflow and associate your Tasks and Workflows.

```python
taskflow = Taskflow()

# These function take arrays of Workflows and Tasks (not associated to a Workflow) and add them to Taskflow
taskflow.add_workflows(workflows)
taskflow.add_tasks(tasks)
```

The above code is usually all you need to setup Taskflow, see the exmaple directory for an example implementation.

## CLI

The CLI is used to manage Taskflow, manage the database, run the scheduler, run the pull worker, and execute task instances.

```
Usage: main.py [OPTIONS] COMMAND [ARGS]...

Options:
  --taskflow TEXT
  --help           Show this message and exit.

Commands:
  api_server
  init_db
  migrate_db
  pull_worker
  queue_task
  queue_workflow
  run_task
  scheduler
```
