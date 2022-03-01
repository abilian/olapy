from setuptools import find_packages
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
from Cython import __version__ as cython_version

import re
import ast

from os.path import abspath, dirname, exists, join
from os import makedirs, chdir, getcwd
from shutil import copy, copytree, rmtree
from subprocess import run

NAME = "olapy"
BUILD = abspath(dirname(__file__))
ROOT = abspath(join(dirname(__file__), ".."))
LIBFMT = abspath(join(ROOT, "libfmt"))


def read(*path):
    full_path = join(BUILD, *path)
    if not exists(full_path):
        return ""
    with open(full_path, "r", encoding="utf-8") as f:
        return f.read()


def read_version():
    version_re = re.compile(r"__version__\s+=\s+(.*)")
    version_string = version_re.search(read(NAME, "core", "common.py")).group(1)
    return str(ast.literal_eval(version_string))


def build_libfmt():
    if exists(join(LIBFMT, "libfmt.a")):
        print("libfmt.a found:", join(LIBFMT, "libfmt.a"))
        return
    print("Build libfmt 8.0.1")
    if not exists(LIBFMT):
        makedirs(LIBFMT)
    src = "fmt-8.0.1"
    src_path = join(ROOT, "vendor", f"{src}.tar.gz")
    if not exists(src_path):
        raise ValueError(f"{src_path} not found")
    build = join(BUILD, "build_fmt")
    if exists(build):
        rmtree(build)
    makedirs(build)
    orig_wd = getcwd()
    chdir(build)
    run(["tar", "xzf", src_path])
    chdir(join(build, src))
    run(["cmake", "-DCMAKE_POSITION_INDEPENDENT_CODE=TRUE", "."])
    run(["make", "fmt"])
    chdir(orig_wd)
    copytree(join(build, src, "include", "fmt"), join(LIBFMT, "fmt"))
    copy(join(build, src, "libfmt.a"), LIBFMT)


def pypyx_ext(*pathname):
    src = join(*pathname) + ".pyx"
    if not exists(src):
        src = join(*pathname) + ".py"
    if not exists(src):
        raise ValueError(f"file not found: {src}")
    return Extension(
        ".".join(pathname),
        sources=[src],
        language="c++",
        extra_compile_args=[
            "-pthread",
            "-std=c++17",
            "-O3",
            "-Wno-unused-function",
            "-Wno-deprecated-declarations",
        ],
        libraries=["fmt"],
        include_dirs=["libfmt"],
        library_dirs=["libfmt"],
    )


extensions = [
    pypyx_ext(NAME, "stdlib", "xml_utils"),
    pypyx_ext(NAME, "cypxml", "cypxml"),
    pypyx_ext(NAME, "core", "common"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader_custom"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader_db"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader"),
    pypyx_ext(NAME, "core", "mdx", "executor", "mdx_engine_lite"),
    pypyx_ext(NAME, "core", "mdx", "executor", "mdx_engine"),
    pypyx_ext(NAME, "core", "mdx", "executor", "utils"),
    pypyx_ext(NAME, "core", "services", "dict_discover_request_handler"),
    pypyx_ext(NAME, "core", "services", "dict_execute_request_handler"),
    pypyx_ext(NAME, "core", "services", "models"),
    pypyx_ext(NAME, "core", "services", "request_properties_models"),
    pypyx_ext(NAME, "core", "services", "xmla_discover_request_handler"),
    pypyx_ext(NAME, "core", "services", "xmla_discover_request_utils"),
    pypyx_ext(NAME, "core", "services", "xmla_discover_xsds"),
    pypyx_ext(NAME, "core", "services", "xmla_execute_request_handler"),
    pypyx_ext(NAME, "core", "services", "xmla_execute_xsds"),
    pypyx_ext(NAME, "core", "services", "xmla_lib"),
    pypyx_ext(NAME, "core", "services", "xmla"),
]

readme = read("README.rst")
version = read_version()

build_libfmt()
build_libfmt = join(BUILD, "libfmt")
if exists(build_libfmt):
    rmtree(build_libfmt)
copytree(LIBFMT, build_libfmt)

print("Build", NAME, version, "in", BUILD, "with Cython", cython_version)

setup(
    ext_modules=cythonize(
        extensions,
        language_level="3str",
        include_path=[
            join(BUILD, NAME, "stdlib"),
            join(BUILD, NAME),
        ],
    ),
    name=NAME,
    version=version,
    description="OlaPy, an experimental OLAP engine based on Pandas",
    long_description=readme,
    python_requires="<4,>=3.9",
    project_urls={"homepage": "https://github.com/abilian/olapy"},
    url="https://github.com/abilian/olapy",
    author="Abilian SAS",
    author_email="jd@abilian.com",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)",
        "Natural Language :: English",
        "Operating System :: Linux",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CythonPlus",
    ],
    license="GNU LGPL",
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": ["olapy = olapy.__main__:cli", "etl = olapy.etl.etl:run_etl"]
    },
    packages=[
        "olapy.default_config",
        "olapy.demo_cubes_templates",
        "olapy",
        "olapy.cypxml",
        "olapy.stdlib",
        "olapy.core",
        "olapy.core.mdx",
        "olapy.core.mdx.executor",
        "olapy.core.mdx.executor.spark",
        "olapy.core.mdx.parser",
        "olapy.core.mdx.tools",
        "olapy.core.services",
        "olapy.core.services.spark",
        "olapy.etl",
        # "demos.ETL",
        # "micro_bench",
        # "micro_bench.olapy_pandas_VS_olapy_pyspark",
    ],
    package_dir={"": "."},
    package_data={
        "config": ["*.yml"],
        "cubes_templates": [
            "*.sql",
            "*.txt",
            "*.yml",
            "Black_Friday/*.csv",
            "foodmart/*.csv",
            "foodmart_with_config/*.csv",
            "sales/*.csv",
        ],
        "demos.ETL": ["*.xlsx", "*.yml"],
        "micro_bench": [
            "db_dumps/*.bak",
            "db_dumps/*.sql",
            "db_dumps/oracle_dump/*.sql",
        ],
        "micro_bench.olapy_pandas_VS_olapy_pyspark": [
            "*.rst",
            "*.txt",
            "benchmark_result/*.bat",
            "benchmark_result/source/*.rst",
            "benchmark_result/source/img/*.png",
            "img/*.png",
        ],
        "olapy.etl": ["*.yml", "input_dir/*.csv", "input_dir/*.sql", "input_dir/*.txt"],
    },
    install_requires=[
        "attrs",
        "click",
        "lxml",
        "pandas",
        "pyyaml>=4.2b1",
        "regex",
        "spyne==2.*,>=2.13.0",
        "sqlalchemy",
        "typing",
        "xmlwitch",
    ],
    # extras_require={
    #     "dev": [
    #         "black",
    #         "devtools==0.*,>=0.8.0",
    #         "flake8",
    #         "flake8-bugbear",
    #         "flake8-comprehensions",
    #         "flake8-mutable",
    #         "flake8-pytest",
    #         "flake8-super-call",
    #         "flake8-tidy-imports",
    #         "gprof2dot",
    #         "ipdb",
    #         "isort==5.*,>=5.0.0",
    #         "mastool",
    #         "mccabe",
    #         "mypy",
    #         "pre-commit",
    #         "prettytable",
    #         "py-cpuinfo",
    #         "pylint",
    #         "pytest",
    #         "pytest-cov",
    #         "sphinx-click",
    #         "sphinx-rtd-theme",
    #         "tox",
    #         "types-pyyaml==6.*,>=6.0.1",
    #         "types-six==1.*,>=1.16.3",
    #     ],
    #     "etl": [
    #         "awesome-slugify",
    #         "bonobo",
    #         "bonobo-sqlalchemy<0.6.1",
    #         "python-dotenv",
    #         "whistle<1.0.1",
    #     ],
    #     "spark": ["pyspark<3"],
    # },
)
