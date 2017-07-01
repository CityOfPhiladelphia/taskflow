import pytest

from shared_fixtures import *
from restful_ben.test_utils import json_call, login as orig_login, dict_contains, iso_regex

def login(*args, **kwargs):
    kwargs['path'] = '/v1/session'
    return orig_login(*args, **kwargs)

def test_list_workflows(app):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/workflows')
    assert response.status_code == 200
    assert response.json['count'] == 2
    assert response.json['page'] == 1
    assert response.json['total_pages'] == 1
    assert len(response.json['data']) == 2
    assert dict_contains(response.json['data'][0], {
        'name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': '0 6 * * *',
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })
    assert dict_contains(response.json['data'][1], {
        'name': 'workflow2',
        'active': True,
        'title': None,
        'description': None,
        'schedule': None,
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })

def test_get_workflow(app):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/workflows/workflow1')
    assert response.status_code == 200
    assert dict_contains(response.json, {
        'name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': '0 6 * * *',
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })

def test_list_tasks(app):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/tasks')
    assert response.status_code == 200
    assert response.json['count'] == 4
    assert response.json['page'] == 1
    assert response.json['total_pages'] == 1
    assert len(response.json['data']) == 4
    assert dict_contains(response.json['data'][0], {
        'name': 'task1',
        'workflow_name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': None,
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })
    assert dict_contains(response.json['data'][1], {
        'name': 'task2',
        'workflow_name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': None,
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })
    assert dict_contains(response.json['data'][2], {
        'name': 'task3',
        'workflow_name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': None,
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })
    assert dict_contains(response.json['data'][3], {
        'name': 'task4',
        'workflow_name': 'workflow1',
        'active': True,
        'title': None,
        'description': None,
        'schedule': None,
        'start_date': None,
        'end_date': None,
        'concurrency': 1,
        'sla': None,
        'default_priority': 'normal'
    })

def test_create_workflow_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    workflow_instance = {
        'workflow_name': 'workflow2',
        'unique': 'user-32324-payment-973794'
    }

    response = json_call(test_client.post, '/v1/workflow-instances', workflow_instance, headers={'X-CSRF': csrf_token})
    assert response.status_code == 201
    assert dict_contains(response.json, {
        'id': 2,
        'workflow_name': 'workflow2',
        'status': 'queued',
        'run_at': iso_regex,
        'unique': 'user-32324-payment-973794',
        'params': None,
        'priority': 'normal',
        'started_at': None,
        'scheduled': False,
        'ended_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_get_workflow_instance(app, instances):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/workflow-instances/1')
    assert response.status_code == 200
    assert dict_contains(response.json, {
        'id': 1,
        'workflow_name': 'workflow1',
        'status': 'running',
        'run_at': '2017-06-03T06:00:00+00:00',
        'unique': None,
        'params': None,
        'priority': 'normal',
        'started_at': '2017-06-03T06:00:00+00:00',
        'scheduled': True,
        'ended_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_list_workflow_instances(app, instances):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/workflow-instances')
    assert response.status_code == 200
    assert response.json['count'] == 1
    assert response.json['page'] == 1
    assert response.json['total_pages'] == 1
    assert len(response.json['data']) == 1
    assert dict_contains(response.json['data'][0], {
        'id': 1,
        'workflow_name': 'workflow1',
        'status': 'running',
        'run_at': '2017-06-03T06:00:00+00:00',
        'unique': None,
        'params': None,
        'priority': 'normal',
        'started_at': '2017-06-03T06:00:00+00:00',
        'scheduled': True,
        'ended_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_update_workflow_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    response = json_call(test_client.get, '/v1/workflow-instances/1')
    assert response.status_code == 200

    workflow_instance = response.json
    workflow_instance['priority'] = 'high'
    previous_updated_at = response.json['updated_at']

    response = json_call(test_client.put, '/v1/workflow-instances/1', workflow_instance, headers={'X-CSRF': csrf_token})
    assert response.status_code == 200
    assert dict_contains(response.json, {
        'id': 1,
        'workflow_name': 'workflow1',
        'status': 'running',
        'run_at': iso_regex,
        'unique': None,
        'params': None,
        'priority': 'high',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
    assert response.json['updated_at'] > previous_updated_at

def test_delete_workflow_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    response = json_call(test_client.get, '/v1/workflow-instances/1')
    assert response.status_code == 200

    response = json_call(test_client.get, '/v1/task-instances?workflow_instance_id=1')
    assert response.status_code == 200
    assert response.json['count'] == 4

    response = json_call(test_client.delete, '/v1/workflow-instances/1', headers={'X-CSRF': csrf_token})
    assert response.status_code == 204

    response = json_call(test_client.get, '/v1/workflow-instances/1')
    assert response.status_code == 404

    response = json_call(test_client.get, '/v1/task-instances?workflow_instance_id=1')
    assert response.status_code == 200
    assert response.json['count'] == 0

def test_create_task_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    task_instance = {
        'task_name': 'task1',
        'unique': 'user-32324-payment-973794'
    }

    response = json_call(test_client.post, '/v1/task-instances', task_instance, headers={'X-CSRF': csrf_token})
    assert response.status_code == 201
    assert dict_contains(response.json, {
        'id': 5,
        'task_name': 'task1',
        'workflow_instance_id': None,
        'status': 'queued',
        'run_at': iso_regex,
        'unique': 'user-32324-payment-973794',
        'params': {},
        'priority': 'normal',
        'started_at': None,
        'scheduled': False,
        'ended_at': None,
        'attempts': 0,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_get_task_instance(app, instances):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/task-instances/1')
    assert response.status_code == 200
    assert dict_contains(response.json, {
        'id': 1,
        'task_name': 'task1',
        'workflow_instance_id': 1,
        'status': 'success',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': iso_regex,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_list_task_instances(app, instances):
    test_client = app.test_client()
    login(test_client)

    response = json_call(test_client.get, '/v1/task-instances')
    assert response.status_code == 200
    assert response.json['count'] == 4
    assert response.json['page'] == 1
    assert response.json['total_pages'] == 1
    assert len(response.json['data']) == 4
    assert dict_contains(response.json['data'][0], {
        'id': 1,
        'task_name': 'task1',
        'workflow_instance_id': 1,
        'status': 'success',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': iso_regex,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
    assert dict_contains(response.json['data'][1], {
        'id': 2,
        'task_name': 'task2',
        'workflow_instance_id': 1,
        'status': 'success',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': iso_regex,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
    assert dict_contains(response.json['data'][2], {
        'id': 3,
        'task_name': 'task3',
        'workflow_instance_id': 1,
        'status': 'success',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': iso_regex,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
    assert dict_contains(response.json['data'][3], {
        'id': 4,
        'task_name': 'task4',
        'workflow_instance_id': 1,
        'status': 'running',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': None,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': None,
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })

def test_update_task_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    response = json_call(test_client.get, '/v1/task-instances/1')
    assert response.status_code == 200

    task_instance = response.json
    task_instance['worker_id'] = 'foo'
    previous_updated_at = task_instance['updated_at']

    response = json_call(test_client.put, '/v1/task-instances/1', task_instance, headers={'X-CSRF': csrf_token})
    assert response.status_code == 200
    assert dict_contains(response.json, {
        'id': 1,
        'task_name': 'task1',
        'workflow_instance_id': 1,
        'status': 'success',
        'run_at': iso_regex,
        'unique': None,
        'params': {},
        'priority': 'normal',
        'started_at': iso_regex,
        'scheduled': True,
        'ended_at': iso_regex,
        'attempts': 1,
        'max_attempts': 1,
        'timeout': 300,
        'retry_delay': 300,
        'push': False,
        'push_state': None,
        'worker_id': 'foo',
        'locked_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
    assert response.json['updated_at'] > previous_updated_at

def test_delete_task_instance(app, instances):
    test_client = app.test_client()
    csrf_token = login(test_client)

    response = json_call(test_client.get, '/v1/task-instances/1')
    assert response.status_code == 200

    response = json_call(test_client.delete, '/v1/task-instances/1', headers={'X-CSRF': csrf_token})
    assert response.status_code == 204

    response = json_call(test_client.get, '/v1/task-instances/1')
    assert response.status_code == 404

def test_get_recurring_latest(app, instances, dbsession):
    test_client = app.test_client()
    login(test_client)

    workflow_instance = WorkflowInstance(
        workflow_name='workflow1',
        scheduled=True,
        run_at=datetime(2017, 6, 4, 6),
        started_at=datetime(2017, 6, 4, 6),
        status='running',
        priority='normal')
    dbsession.add(workflow_instance)
    dbsession.commit()

    response = json_call(test_client.get, '/v1/workflow-instances/recurring-latest')
    assert response.status_code == 200
    assert dict_contains(response.json[0], {
        'id': 2,
        'workflow_name': 'workflow1',
        'status': 'running',
        'run_at': '2017-06-04T06:00:00+00:00',
        'unique': None,
        'params': None,
        'priority': 'normal',
        'started_at': '2017-06-04T06:00:00+00:00',
        'scheduled': True,
        'ended_at': None,
        'created_at': iso_regex,
        'updated_at': iso_regex
    })
