from __future__ import absolute_import, division, print_function, unicode_literals

import sys

import click
from olapy.core.services.xmla import runserver


def main(arg):
    """
    Execute xmla provider.

    :param arg: -c | --console :  show logs in server console
    :return:
    """
    if len(arg) > 1:
        if arg[1] in ("-c", "--console"):
            runserver(write_on_file=False)
        else:
            print('invalide argument !')
    else:
        runserver(write_on_file=True)


@click.group()
def cli():
    pass


cli.add_command(runserver)

if __name__ == "__main__":
    main(sys.argv)
