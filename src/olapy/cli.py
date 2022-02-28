import os
import sys
from distutils.dir_util import copy_tree
from os.path import dirname, isdir, isfile, join, realpath
from shutil import copyfile

import click

from olapy.core.common import OLAPY_PATH, DEFAULT_CUBES, DEFAULT_CONFIG


@click.command()
def init():
    print("Starting Olapy initialization")
    print("using OLAPY_PATH:", OLAPY_PATH)

    olapy_lib_dir = realpath(join("..", dirname(realpath(__file__))))

    if not isdir(DEFAULT_CUBES):
        print("Initializing demo cubes")
        os.makedirs(DEFAULT_CUBES)

        src_cubes_dir = join(olapy_lib_dir, "demo_cubes_templates")
        if not isdir(src_cubes_dir):
            print("'cubes_templates' folder not found, exiting.")
            sys.exit(1)

        copy_tree(
            src_cubes_dir,
            DEFAULT_CUBES,
        )

    if not isfile(DEFAULT_CONFIG):
        print("Initializing config file with default")

        src_olapy_config = join(olapy_lib_dir, "default_config", "olapy-config.yml")
        if not isfile(src_olapy_config):
            print("default config file folder not found, exiting.")
            sys.exit(1)

        copyfile(
            src_olapy_config,
            DEFAULT_CONFIG,
        )


if __name__ == "__main__":
    init()
