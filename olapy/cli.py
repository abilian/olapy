from __future__ import absolute_import, division, print_function

import os
from distutils.dir_util import copy_tree
from os.path import dirname, expanduser
from shutil import copyfile

import click


@click.command()
def init():
    if "OLAPY_PATH" in os.environ:
        home_directory = os.environ["OLAPY_PATH"]
    else:
        home_directory = expanduser("~")

    olapy_lib_dir = dirname(
        os.path.join(os.path.dirname(os.path.realpath(__file__))), )

    if not os.path.isdir(os.path.join(home_directory, "olapy-data", "cubes")):
        os.makedirs(os.path.join(home_directory, "olapy-data", "cubes"))

        copy_tree(
            os.path.join(olapy_lib_dir, "cubes_templates"),
            os.path.join(home_directory, "olapy-data", "cubes"),
        )
        print("Initializing demo cubes")

    if not os.path.isfile(
            os.path.join(home_directory, "olapy-data", "olapy-config.yml"), ):
        copyfile(
            os.path.join(olapy_lib_dir, "config", "olapy-config.yml"),
            os.path.join(home_directory, "olapy-data", "olapy-config.yml"),
        )
        print("Initializing config files")


if __name__ == "__main__":
    init()
