from datetime import datetime

from marshmallow import Schema, fields
from marshmallow_sqlalchemy import ModelSchema, field_for
from sqlalchemy import text
from flask import request
from flask_restful import Resource, abort
from flask_login import login_required
from restful_ben.resources import (
    RetrieveUpdateDeleteResource,
    QueryEngineMixin,
    CreateListResource
)
from restful_ben.auth import (
    SessionResource,
    authorization,
    CSRF
)

from taskflow import Taskflow, Workflow, WorkflowInstance, Task, TaskInstance
from taskflow.core.models import User

csrf = CSRF()

class LocalSessionResource(SessionResource):
    User = User
    csrf = csrf

standard_authorization = authorization({
    'normal': ['GET'],
    'admin': ['POST','PUT','GET','DELETE']
})

def to_list_response(data):
    count = len(data)
    return {
        'data': data,
        'count': count,
        'total_pages': 1 if count > 0 else 0,
        'page': 1 if count > 0 else 0,
    }

class SchedulableSchema(Schema):
    name = fields.String(dump_only=True)
    active = fields.Boolean(required=True)
    title = fields.String(dump_only=True)
    description = fields.String(dump_only=True)
    concurrency = fields.Integer(dump_only=True)
    sla = fields.Integer(dump_only=True)
    schedule = fields.String(dump_only=True)
    default_priority = fields.String(dump_only=True)
    start_date = fields.DateTime(dump_only=True)
    end_date = fields.DateTime(dump_only=True)

class TaskSchema(SchedulableSchema):
    workflow_name = fields.String(dump_only=True)

class WorkflowSchema(SchedulableSchema):
    pass

workflow_schema = WorkflowSchema()
workflows_schema = WorkflowSchema(many=True)

class WorkflowListResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self):
        self.taskflow.sync_db(self.session, read_only=True)
        workflows = sorted(self.taskflow.get_workflows(), key=lambda workflow: workflow.name)
        workflows_data = workflows_schema.dump(workflows).data
        return to_list_response(workflows_data)

class WorkflowResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self, workflow_name):
        self.taskflow.sync_db(self.session, read_only=True)
        workflow = self.taskflow.get_workflow(workflow_name)
        return workflow_schema.dump(workflow).data

    def put(self, workflow_name):
        input_workflow = workflow_schema.load(request.json or {})

        if input_workflow.errors:
            abort(400, errors=input_workflow.errors)

        self.taskflow.sync_db(self.session, read_only=True)
        workflow = self.taskflow.get_workflow(workflow_name)

        if not workflow:
            abort(404)

        workflow.active = input_workflow.data['active']
        self.session.commit()

        return workflow_schema.dump(workflow).data

task_schema = TaskSchema()
tasks_schema = TaskSchema(many=True)

class TaskListResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self):
        self.taskflow.sync_db(self.session, read_only=True)
        tasks = list(self.taskflow.get_tasks())
        for workflow in self.taskflow.get_workflows():
            tasks += workflow.get_tasks()
        tasks_sorted = sorted(tasks, key=lambda task: task.name)
        tasks_data = tasks_schema.dump(tasks_sorted).data
        return to_list_response(tasks_data)

class TaskResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self, task_name):
        self.taskflow.sync_db(self.session, read_only=True)
        task = self.taskflow.get_task(task_name)
        return task_schema.dump(task).data

    def put(self, task_name):
        input_task = task_schema.load(request.json or {})

        if input_task.errors:
            abort(400, errors=input_task.errors)

        self.taskflow.sync_db(self.session, read_only=True)
        task = self.taskflow.get_task(task_name)

        if not task:
            abort(404)

        task.active = input_task.data['active']
        self.session.commit()

        return task_schema.dump(task).data

## Instances

class WorkflowInstanceSchema(ModelSchema):
    class Meta:
        model = WorkflowInstance
        exclude = ['task_instances']

    id = field_for(WorkflowInstance, 'id', dump_only=True)
    status = field_for(WorkflowInstance, 'status', dump_only=True)
    scheduled = field_for(WorkflowInstance, 'scheduled', dump_only=True)
    created_at = field_for(WorkflowInstance, 'created_at', dump_only=True)
    updated_at = field_for(WorkflowInstance, 'updated_at', dump_only=True)

workflow_instance_schema = WorkflowInstanceSchema()
workflow_instances_schema = WorkflowInstanceSchema(many=True)

class WorkflowInstanceResource(RetrieveUpdateDeleteResource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]
    single_schema = workflow_instance_schema
    model = WorkflowInstance

class WorkflowInstanceListResource(QueryEngineMixin, CreateListResource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]
    single_schema = workflow_instance_schema
    many_schema = workflow_instances_schema
    model = WorkflowInstance

    def post(self):
        if 'workflow_name' in request.json:
            workflow = self.taskflow.get_workflow(request.json['workflow_name'])
            if not workflow:
                abort(400, errors={'workflow_name': 'Workflow `{}` not found'.format(request.json['workflow_name'])})

            if 'priority' not in request.json:
                request.json['priority'] = workflow.default_priority

        return super(WorkflowInstanceListResource, self).post()

class RecurringWorkflowLastestResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self):
        workflow_instances = self.session.query(WorkflowInstance)\
            .from_statement(text("""
                SELECT workflow_instances.* FROM workflow_instances
                INNER JOIN (
                    SELECT workflow_name, MAX(started_at) AS max_date
                    FROM workflow_instances
                    WHERE status != 'queued' AND scheduled = true
                    GROUP BY workflow_name) AS tp
                ON workflow_instances.workflow_name = tp.workflow_name AND
                   workflow_instances.started_at = tp.max_date
                ORDER BY workflow_instances.workflow_name;
            """))\
            .all()

        return workflow_instances_schema.dump(workflow_instances).data

class TaskInstanceSchema(ModelSchema):
    class Meta:
        model = TaskInstance
        exclude = ['workflow']

    id = field_for(TaskInstance, 'id', dump_only=True)
    workflow_instance_id = field_for(TaskInstance, 'workflow_instance_id', dump_only=True)
    status = field_for(TaskInstance, 'status', dump_only=True)
    scheduled = field_for(WorkflowInstance, 'scheduled', dump_only=True)
    created_at = field_for(TaskInstance, 'created_at', dump_only=True)
    updated_at = field_for(TaskInstance, 'updated_at', dump_only=True)

task_instance_schema = TaskInstanceSchema()
task_instances_schema = TaskInstanceSchema(many=True)

class TaskInstanceResource(RetrieveUpdateDeleteResource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]
    single_schema = task_instance_schema
    model = TaskInstance

class TaskInstanceListResource(QueryEngineMixin, CreateListResource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]
    single_schema = task_instance_schema
    many_schema = task_instances_schema
    model = TaskInstance

    def post(self):
        if 'task_name' in request.json:
            task = self.taskflow.get_task(request.json['task_name'])
            if not task:
                abort(400, errors={'task_name': 'Task `{}` not found'.format(request.json['task_name'])})

            request.json['push'] = task.push_destination != None

            if 'priority' not in request.json:
                request.json['priority'] = task.default_priority

            if 'max_attempts' not in request.json:
                request.json['max_attempts'] = task.retries + 1

            if 'timeout' not in request.json:
                request.json['timeout'] = task.timeout

            if 'retry_delay' not in request.json:
                request.json['retry_delay'] = task.retry_delay

        return super(TaskInstanceListResource, self).post()

class RecurringTaskLastestResource(Resource):
    method_decorators = [csrf.csrf_check, standard_authorization, login_required]

    def get(self):
        task_instances = self.session.query(TaskInstance)\
            .from_statement(text("""
                SELECT task_instances.* FROM task_instances
                INNER JOIN (
                    SELECT task_name, MAX(started_at) AS max_date
                    FROM task_instances
                    WHERE status != 'queued' AND scheduled = true
                    GROUP BY task_name) AS tp
                ON task_instances.task_name = tp.task_name AND
                   task_instances.started_at = tp.max_date
                ORDER BY task_instances.task_name;
            """))\
            .all()

        return task_instances_schema.dump(task_instances).data
