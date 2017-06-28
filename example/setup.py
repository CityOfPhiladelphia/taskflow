#!/usr/bin/env python

from distutils.core import setup

setup(
    name='taskflow_basic_example',
    version='0.1dev',
    packages=[
        'taskflow_basic_example',
        'taskflow_basic_example.workflows',
        'taskflow_basic_example.tasks'
    ],
    install_requires=[
    ],
    dependency_links=[
    ],
)
