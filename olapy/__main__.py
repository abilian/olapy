from __future__ import absolute_import, division, print_function, \
    unicode_literals

import sys

import click

from .cli import init
from .core.services.xmla import runserver


def main(arg):
    """Execute xmla provider.

    :param arg: -c | --console :  show logs in server console
    :return:
    """
    runserver()


@click.group()
def cli():
    pass


cli.add_command(runserver)
cli.add_command(init)

if __name__ == "__main__":
    main(sys.argv)
