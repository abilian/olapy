# -*- coding: utf-8 -*-
from __future__ import absolute_import

from pkg_resources import parse_requirements
from setuptools import find_packages, setup

_install_requires = parse_requirements(open("requirements.in"))
install_requires = [str(req) for req in _install_requires]

VERSION = "0.7.8"

setup(
    name="olapy",
    version=VERSION,
    packages=find_packages(),
    author="Abilian SAS",
    author_email="contact@abilian.com",
    description="OLAP Engine",
    url="https://github.com/abilian/olapy",
    long_description=open("README.rst").read(),
    package_data={"olapy": ["config/olapy-config.yml"]},
    include_package_data=True,
    install_requires=install_requires,
    data_files=[("config", ["config/olapy-config.yml"])],
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        # "Topic :: Business intelligence",
    ],
    extras_require={
        "etl": [
            "bonobo",
            "bonobo-sqlalchemy<0.6.1",
            "awesome-slugify",
            "python-dotenv",
            "whistle<1.0.1",
        ],
        "spark": ["pyspark<3"],
    },
    entry_points={
        "console_scripts": ["olapy = olapy.__main__:cli", "etl = olapy.etl.etl:run_etl"]
    },
    # whistle<1.0.1 # used by bonobo, 1.0.1 does not work well with py2.7
)
