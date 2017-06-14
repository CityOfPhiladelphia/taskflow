#!/usr/bin/env python

from distutils.core import setup

setup(
    name='taskflow',
    version='0.0.2',
    packages=[
        'taskflow',
        'taskflow.core',
        'taskflow.push_workers',
        'taskflow.tasks'
    ],
    install_requires=[
        'alembic==0.9.2',
        'boto3==1.4.4',
        'click==6.7',
        'croniter==0.3.17',
        'psycopg2==2.7.1',
        'pytest==3.1.1',
        'requests==2.17.3',
        'smart-open==1.5.3',
        'SQLAlchemy==1.1.10',
        'toposort==1.5'
    ],
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/jsontableschema-sql-py/tarball/master#egg=jsontableschema_sql-0.8.0'
    ],
)
