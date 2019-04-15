.PHONY: test unit full-test clean setup stage deploy


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

	@echo "Running pylint, some errors reported might be false positives"
	-pylint -E --rcfile .pylint.rc $(SRC)

lint-py3k:
	@echo "Checking Py3k (basic) compatibility"
	-pylint --py3k *.py $(SRC) tests


lint-mypy:
	mypy olapy tests

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
	rm -rf .tox

format:
	black $(SRC) tests micro_bench demos *.py
	isort -rc $(SRC) tests micro_bench demos *.py

update-deps:
	pip-compile -U > /dev/null
	pip-compile > /dev/null
	git --no-pager diff requirements.txt

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
	rm -rf /tmp/olapy
	git clone . /tmp/olapy
	cd /tmp/olapy ; python setup.py sdist
	cd /tmp/olapy ; python setup.py sdist upload
