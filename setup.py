# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import os

from pip.download import PipSession
from pip.req import parse_requirements
from setuptools import find_packages, setup

RUNNING_TOX = 'RUNNING_TOX' in os.environ

session = PipSession()
_install_requires = parse_requirements('requirements.in', session=session)
install_requires = [str(ir.req) for ir in _install_requires]

setup(
    name='olapy',
    version="0.5.0",
    packages=find_packages(),
    author="Abilian SAS",
    author_email="contact@abilian.com",
    description="OLAP Engine",
    url='https://github.com/abilian/olapy',
    long_description=open('README.rst').read(),
    package_data={"olapy": ['config/olapy-config']},
    include_package_data=True,
    install_requires=install_requires,
    data_files=[('config', ['config/olapy-config'])],
    classifiers=[
        "Programming Language :: Python",
        'Development Status :: 3 - Alpha',
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6"
        # "Topic :: Business intelligence",
    ],
    entry_points={
        'console_scripts': [
            'olapy = olapy.__main__:cli'
        ]
    },
)
