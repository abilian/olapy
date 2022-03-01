"""Some functions or constants commonly used through the code
"""
import os
from os.path import expanduser, join
from pathlib import Path
import platform

__version__ = "0.8.4"
OLAPY_PATH = os.environ.get("OLAPY_PATH", expanduser("~"))
DEFAULT_DATA = join(OLAPY_PATH, "olapy-data")
DEFAULT_CUBES = join(DEFAULT_DATA, "cubes")
DEFAULT_CUBES_CONFIG = join(DEFAULT_CUBES, "cubes-config.yml")
DEFAULT_CONFIG = join(DEFAULT_DATA, "olapy-config.yml")
DEFAULT_LOG_DIR = join(DEFAULT_DATA, "logs")


def is_compiled():
    return Path(__file__).suffix == ".so"


print("OlaPy instance version:", __version__)
print("Default data path:", DEFAULT_DATA)
print("Version of Python:", platform.python_version())
if is_compiled():
    print("This OlaPy version is compiled with Cython+")
else:
    print("This OlaPy version use standard CPython")
