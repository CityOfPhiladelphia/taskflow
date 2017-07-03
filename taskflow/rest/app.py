import os

import flask
from flask_restful import Api
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

from taskflow.rest import resources
from taskflow.core.models import metadata, BaseModel, User

def create_app(taskflow_instance, connection_string=None, secret_key=None):
    app = flask.Flask(__name__)
    app.config['DEBUG'] = os.getenv('DEBUG', False)
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_DATABASE_URI'] = connection_string or os.getenv('SQLALCHEMY_DATABASE_URI')
    app.config['SESSION_COOKIE_NAME'] = 'taskflowsession'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['PERMANENT_SESSION_LIFETIME'] = 43200
    app.config['SECRET_KEY'] = secret_key or os.getenv('FLASK_SESSION_SECRET_KEY')

    db = SQLAlchemy(metadata=metadata, model_class=BaseModel)

    db.init_app(app)
    api = Api(app)
    CORS(app, supports_credentials=True)

    login_manager = LoginManager()
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.query(User).filter(User.id == user_id).first()

    def apply_attrs(class_def, attrs):
        for key, value in attrs.items():
            setattr(class_def, key, value)
        return class_def

    attrs = {
        'session': db.session,
        'taskflow': taskflow_instance
    }

    with app.app_context():
        api.add_resource(apply_attrs(resources.LocalSessionResource, attrs), '/v1/session')

        api.add_resource(apply_attrs(resources.WorkflowListResource, attrs), '/v1/workflows')
        api.add_resource(apply_attrs(resources.WorkflowResource, attrs), '/v1/workflows/<workflow_name>')

        api.add_resource(apply_attrs(resources.TaskListResource, attrs), '/v1/tasks')
        api.add_resource(apply_attrs(resources.TaskResource, attrs), '/v1/tasks/<task_name>')

        api.add_resource(apply_attrs(resources.WorkflowInstanceListResource, attrs), '/v1/workflow-instances')
        api.add_resource(apply_attrs(resources.WorkflowInstanceResource, attrs), '/v1/workflow-instances/<int:instance_id>')

        api.add_resource(apply_attrs(resources.RecurringWorkflowLastestResource, attrs), '/v1/workflow-instances/recurring-latest')

        api.add_resource(apply_attrs(resources.TaskInstanceListResource, attrs), '/v1/task-instances')
        api.add_resource(apply_attrs(resources.TaskInstanceResource, attrs), '/v1/task-instances/<int:instance_id>')

        api.add_resource(apply_attrs(resources.RecurringTaskLastestResource, attrs), '/v1/task-instances/recurring-latest')

    return app
