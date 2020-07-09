.PHONY: default run test test-with-coverage clean develop lint tidy format

SRC=olapy
PKG=$(SRC)

default: test lint

run:
	python manage.py runserver

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
	pip install -q pip-tools
	pip-sync requirements.txt
	pip install -q -r requirements.txt -r dev-requirements.txt
	pip install -e .
	@echo "--> Activating pre-commit hook"
	pre-commit install
	@echo ""

#
# Linting
#
lint: lint-python

lint-python:
	@echo "--> Linting Python files"
	flake8 olapy tests

	@make lint-py3k
	@make lint-mypy
	# @make lint-pylint

lint-pylint:
	@echo "Running pylint, some errors reported might be false positives"
	-pylint -E --rcfile .pylint.rc $(SRC)

lint-py3k:
	@echo "Checking Py3k (basic) compatibility"
	pylint --py3k -d W1637 *.py $(SRC) tests

lint-mypy:
	mypy olapy tests


#
# Cleanup
#
clean:
	find . -name "*.pyc" -delete
	find . -name .DS_Store -delete
	find . -name cache -type d -delete
	find . -type d -empty -delete
	rm -rf .mypy_cache
	rm -f migration.log
	rm -rf build dist
	rm -rf *.egg .coverage
	rm -rf doc/_build
	rm -rf static/gen static/.webassets-cache instance/webassets
	rm -rf htmlcov

tidy: clean
	rm -rf .tox .nox

format:
	black $(SRC) tests micro_bench demos *.py
	isort -rc $(SRC) tests micro_bench demos *.py

update-deps:
	poetry update

#
# update deps in windows OS
#
update-deps-win:
	pip-compile -U
	git --no-pager diff requirements.txt

sync-deps:
	pip install -r requirements.txt -r dev-requirements.txt -e .

release:
	git push --tags
	rm -rf /tmp/olapy-dist
	git clone . /tmp/olapy-dist
	cd /tmp/olapy-dist ; python setup.py sdist
	cd /tmp/olapy-dist ; twine upload dist/*
