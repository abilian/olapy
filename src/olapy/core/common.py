"""Some functions or constants commonly used through the code
"""
import os
from os.path import expanduser, join

__version__ = "0.8.3"
OLAPY_PATH = os.environ.get("OLAPY_PATH", expanduser("~"))
DEFAULT_DATA = join(OLAPY_PATH, "olapy-data")
DEFAULT_CUBES = join(DEFAULT_DATA, "cubes")
DEFAULT_CUBES_CONFIG = join(DEFAULT_CUBES, "cubes-config.yml")
DEFAULT_CONFIG = join(DEFAULT_DATA, "olapy-config.yml")
DEFAULT_LOG_DIR = join(DEFAULT_DATA, "logs")
