from __future__ import absolute_import, division, print_function, unicode_literals

import sys
import click

import os
import zipfile
from os.path import expanduser
from shutil import copyfile

RUNNING_TOX = 'RUNNING_TOX' in os.environ

from olapy.core.services.xmla import runserver

@click.command()
def init():
    if 'OLAPY_PATH' in os.environ:
        home_directory = os.environ['OLAPY_PATH']
    elif RUNNING_TOX:
        home_directory = os.environ.get('HOME_DIR')
    else:
        home_directory = expanduser("~")

    if not os.path.isdir(os.path.join(home_directory, 'olapy-data', 'cubes')):
        os.makedirs(os.path.join(home_directory, 'olapy-data', 'cubes'))
        zip_ref = zipfile.ZipFile('cubes_templates/cubes_temp.zip', 'r')
        zip_ref.extractall(os.path.join(home_directory, 'olapy-data', 'cubes'))
        zip_ref.close()

    if not os.path.isfile(
            os.path.join(home_directory, 'olapy-data', 'olapy-config')):
        copyfile('config/olapy-config',
                 os.path.join(home_directory, 'olapy-data', 'olapy-config'))


def main(arg):
    """
    Execute xmla provider.

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
