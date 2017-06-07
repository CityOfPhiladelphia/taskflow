import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import pytest

from taskflow.core.models import BaseModel

def get_logging():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

@pytest.fixture(scope='session')
def engine():
    return create_engine('postgresql://localhost/taskflow_test')

@pytest.fixture
def tables(engine):
    BaseModel.metadata.create_all(engine)
    yield
    BaseModel.metadata.drop_all(engine)

@pytest.fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # put back the connection to the connection pool
    connection.close()
