import os

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine

from .core.models import BaseModel

def get_alembic_config(connection_string):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scripts_directory = os.path.join(current_dir, 'migrations')
    alembic_config = Config(os.path.join(current_dir, 'alembic.ini'))
    alembic_config.set_main_option('script_location', scripts_directory)
    alembic_config.set_main_option('sqlalchemy.url', connection_string)
    return alembic_config

def migrate_db(connection_string):
    alembic_config = get_alembic_config(connection_string)
    command.upgrade(alembic_config, 'heads')

def init_db(connection_string):
    engine = create_engine(connection_string)

    BaseModel.metadata.create_all(engine)

    alembic_config = get_alembic_config(connection_string)
    command.stamp(alembic_config, 'head')
