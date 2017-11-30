from distutils.dir_util import copy_tree
from os.path import expanduser, dirname
from shutil import copyfile

import click
import os

RUNNING_TOX = 'RUNNING_TOX' in os.environ


@click.command()
def init():
    if 'OLAPY_PATH' in os.environ:
        home_directory = os.environ['OLAPY_PATH']
    elif RUNNING_TOX:
        home_directory = os.environ.get('HOME_DIR')
    else:
        home_directory = expanduser("~")

    olapy_lib_dir = dirname(os.path.join(os.path.dirname(os.path.realpath(__file__))))

    if not os.path.isdir(os.path.join(home_directory, 'olapy-data', 'cubes')):
        os.makedirs(os.path.join(home_directory, 'olapy-data', 'cubes'))

        copy_tree(os.path.join(olapy_lib_dir, 'cubes_templates'),
                  os.path.join(home_directory, 'olapy-data', 'cubes'))

    if not os.path.isfile(os.path.join(home_directory, 'olapy-data', 'olapy-config')):
        copyfile(os.path.join(olapy_lib_dir, 'config', 'olapy-config'),
                 os.path.join(home_directory, 'olapy-data', 'olapy-config'))


if __name__ == '__main__':
    init()
