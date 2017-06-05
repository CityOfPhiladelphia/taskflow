from datetime import datetime
from functools import reduce

from toposort import toposort
from sqlalchemy import or_, and_

from .models import Workflow, WorkflowInstance, Task, TaskInstance

class Scheduler(object):
    def __init__(self, session, taskflow, now_override=None):
        self.session = session
        self.taskflow = taskflow

        self.now_override = now_override

    def queue_task(self, task, run_at):
        if run_at == None:
            run_at = self.now()

        task_instance = task.get_new_instance(scheduled=True,
                                              run_at=run_at)
        self.session.add(task_instance)

    def queue_workflow_task(self, workflow, task_name, workflow_instance, run_at=None):
        if run_at == None:
            run_at = self.now()

        ## TODO: use TaskInstance.unique?

        task = workflow.get_task(task_name)

        task_instance = task.get_new_instance(
            scheduled=True,
            run_at=run_at,
            workflow_instance_id=workflow_instance.id,
            priority=workflow_instance.priority or workflow.default_priority)
        self.session.add(task_instance)

    def queue_workflow_tasks(self, workflow_instance):
        workflow = self.taskflow.get_workflow(workflow_instance.workflow)
        dep_graph = workflow.get_dependencies_graph()
        dep_graph = list(toposort(dep_graph))

        results = self.session.query(TaskInstance)\
                    .filter(TaskInstance.workflow_instance_id == workflow_instance.id).all()
        workflow_task_instances = dict()
        for instance in results:
            workflow_task_instances[instance.task_name] = instance

        ## dep_graph looks like [{'task2', 'task1'}, {'task3'}, {'task4'}]
        ## a list of sets where each set is a parallel step
        total_complete_steps = 0
        failed = False
        for step in dep_graph:
            total_in_step = len(step)
            total_complete = 0
            tasks_to_queue = []

            for task_name in step:
                if task_name in workflow_task_instances:
                    if workflow_task_instances[task_name].status == 'success':
                        total_complete += 1
                    elif workflow_task_instances[task_name].status == 'failed':
                        failed = True
                        break
                else:
                    tasks_to_queue.append(task_name)

            if failed:
                break

            for task_name in tasks_to_queue:
                self.queue_workflow_task(workflow, task_name, workflow_instance)

            if len(tasks_to_queue) > 0 and total_complete == total_in_step:
                raise Exception('Attempting to queue tasks for a completed workflow step')

            if total_complete < total_in_step:
                break
            else:
                total_complete_steps += 1

        if failed:
            workflow_instance.status = 'failed'
            self.session.commit()
        elif total_complete_steps == len(dep_graph):
            workflow_instance.status = 'success'
            self.session.commit()

    def queue_workflow(self, workflow, run_at):
        ## TODO: ensure this is in a transaction with queue_tasks ?
        workflow_instance = workflow.get_new_instance(
            scheduled=True,
            run_at=run_at)
        self.session.add(workflow_instance)
        if workflow_instance.run_at <= self.now():
            self.queue_workflow_tasks(workflow_instance)
        self.session.commit()

    def now(self):
        """Allows for dry runs and tests to use a specific datetime as now"""
        if self.now_override:
            return self.now_override
        return datetime.utcnow()

    def schedule_recurring(self, definition_class):
        """Schedules recurring Workflows or Tasks
           definition_class - Workflow or Task"""

        if definition_class == Workflow:
            instance_class = WorkflowInstance
            fresh_recurring_items = self.taskflow.get_fresh_workflows(self.session)
        elif definition_class == Task:
            instance_class = TaskInstance
            fresh_recurring_items = self.taskflow.get_fresh_tasks(self.session)
        else:
            raise Exception('definition_class must be Workflow or Task')

        now = self.now()

        ## get Workflows or Tasks from Taskflow instance
        recurring_items = filter(lambda item: item.active == True and item.schedule != None,
                                 fresh_recurring_items)

        for item in recurring_items:
            # try: !!! add this back after dev
                if definition_class == Workflow:
                    filters = (instance_class.workflow == item.name,)
                else:
                    filters = (instance_class.task_name == item.name,)
                filters += (instance_class.scheduled == True,)

                ## Get the most recent instance of the recurring item
                ## TODO: order by heartbeat instead ?
                most_recent_instance = self.session.query(instance_class)\
                                        .filter(*filters)\
                                        .order_by(instance_class.run_at.desc())\
                                        .first()

                if not most_recent_instance or most_recent_instance.status in ['success','failed']:
                    if not most_recent_instance: ## first run
                        next_run = item.next_run(base_time=now)
                    else:
                        next_run = item.next_run(base_time=most_recent_instance.run_at)
                        last_run = item.last_run(base_time=now)
                        if last_run > next_run:
                            next_run = last_run

                    if item.start_date and next_run < item.start_date or \
                        item.end_date and next_run > item.end_date:
                        continue

                    if definition_class == Workflow:
                        self.queue_workflow(item, next_run)
                    else:
                        self.queue_task(item, next_run)
            # except Exception as e:
            #     ## TODO: switch to logger
            #     ## TODO: rollback?
            #     print('Exception scheduling Workflow "{}"'.format(item.name))
            #     print(e)

    def move_workflows_forward(self):
        """Moves queued and running workflows forward"""
        now = self.now()

        queued_running_workflow_instances = \
            self.session.query(WorkflowInstance)\
            .filter(or_(WorkflowInstance.status == 'running',
                        and_(WorkflowInstance.status == 'queued',
                             WorkflowInstance.run_at <= now)))\
            .all() ## TODO: paginate?

        for workflow_instance in queued_running_workflow_instances:
            # try: !!! add this back after dev
                if workflow_instance.status == 'queued':
                    workflow_instance.status = 'running'
                    self.queue_workflow_tasks(workflow_instance)
                    self.session.commit()
                elif workflow_instance.status == 'running':
                    ## TODO: timeout queued workflow instances that have gone an interval past their run_at
                    self.queue_workflow_tasks(workflow_instance)
                    self.session.commit()
            # except Exception as e:
            #     ## TODO: switch to logger
            #     ## TODO: rollback?
            #     print('Exception scheduling Workflow "{}"'.format(item.name))
            #     print(e)

    def run(self):
        ## TODO: allow for dry_run

        ##### Workflow scheduling

        self.schedule_recurring(Workflow)

        self.move_workflows_forward()


        ##### Task scheduling - tasks that do not belong to a workflow

        self.schedule_recurring(Task)

        ## TODO: push tasks
