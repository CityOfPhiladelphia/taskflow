#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='taskflow',
    version='0.0.2',
    packages=find_packages(),
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
)
