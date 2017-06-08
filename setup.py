#!/usr/bin/env python

from distutils.core import setup

setup(
    name='taskflow',
    version='0.0.2',
    packages=['taskflow',],
    install_requires=[
    ],
    dependency_links=[
        'https://github.com/CityOfPhiladelphia/jsontableschema-sql-py/tarball/master#egg=jsontableschema_sql-0.8.0'
    ],
)
