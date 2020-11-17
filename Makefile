.PHONY: all default run test test-with-coverage clean develop lint tidy format

SRC=olapy
PKG=$(SRC)

default: test lint

all: default

run:
	olapy runserver

#
# testing
#
test:
	pytest . --durations=10

test-with-coverage:
	pytest --tb=short --cov $(PKG) --cov-report term-missing .

#
# setup
#
develop:
	@echo "--> Installing / updating python dependencies for development"
	poetry install
	@echo "--> Activating pre-commit hook"
	pre-commit install
	@echo ""

#
# Linting
#
lint: lint-python

lint-ci: lint

lint-python:
	@echo "--> Linting Python files"
	@make lint-flake8
	@make lint-mypy

lint-flake8:
	flake8 olapy tests

lint-pylint:
	@echo "Running pylint, some errors reported might be false positives"
	-pylint -E --rcfile .pylint.rc $(SRC)

lint-mypy:
	mypy olapy tests


#
# Cleanup
#
clean:
	rm -rf **/*.pyc **/.DS_Store
	find . -name cache -type d -delete
	find . -type d -empty -delete
	rm -rf .mypy_cache migration.log build dist *.egg .coverage htmlcov
	rm -rf doc/_build static/gen static/.webassets-cache instance/webassets

tidy: clean
	rm -rf .tox .nox

format:
	black $(SRC) tests micro_bench demos *.py
	isort $(SRC) tests micro_bench demos *.py

update-deps:
	poetry update

publish: clean
	poetry build
	twine upload dist/*

#
# update deps in windows OS
#
update-deps-win:
	pip-compile -U
	git --no-pager diff requirements.txt
