from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os

import nox

PYTHON_VERSIONS = ["2.7", "3.6", "3.7", "3.8"]
PACKAGE = "abilian"

travis_python_version = os.environ.get("TRAVIS_PYTHON_VERSION")
if travis_python_version:
    python = [travis_python_version]
else:
    python = PYTHON_VERSIONS

nox.options.reuse_existing_virtualenvs = True


@nox.session(python="3.6")
def lint(session):
    session.run("poetry", "install", "-q", external=True)
    session.install("poetry", "psycopg2-binary")

    session.run("make", "lint-ci", external=True)


@nox.session(python=python)
def pytest(session):
    session.run("poetry", "install", "-q", external=True)
    session.install("psycopg2-binary")

    session.run("pip", "check")
    session.run("pytest", "-q")


@nox.session(python="3.8")
def typeguard(session):
    session.run("poetry", "install", "-q", external=True)
    session.install("psycopg2-binary", "typeguard")

    session.run("pytest", f"--typeguard-packages={PACKAGE}")
