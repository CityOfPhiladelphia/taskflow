#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='taskflow',
    version='0.2.9',
    packages=find_packages(),
    install_requires=[
        'alembic==0.9.6',
        'boto3==1.4.4',
        'click==6.7',
        'cron-descriptor==1.2.10',
        'croniter==0.3.17',
        'Flask==0.12.2',
        'Flask-Cors==3.0.2',
        'Flask-RESTful==0.3.6',
        'Flask-SQLAlchemy==2.2',
        'gunicorn==19.7.1',
        'marshmallow==2.13.5',
        'psycopg2==2.7.1',
        'pytest==3.1.1',
        'restful_ben==0.1.0',
        'requests==2.17.3',
        'smart-open==1.5.5',
        'SQLAlchemy==1.1.10',
        'toposort==1.5'
    ],
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/restful-ben/tarball/0.0.1#egg=restful_ben-0.1.0'
    ],
)
