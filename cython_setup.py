from setuptools import find_packages
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

import re
import ast

from os.path import abspath, dirname, exists, join

NAME = "olapy"
PROJECT_ROOT = abspath(dirname(__file__))


def read(*path):
    full_path = join(PROJECT_ROOT, *path)
    if not exists(full_path):
        return ""
    with open(full_path, "r", encoding="utf-8") as f:
        return f.read()


def read_version():
    version_re = re.compile(r"__version__\s+=\s+(.*)")
    version_string = version_re.search(read(NAME, "core", "common.py")).group(1)
    return str(ast.literal_eval(version_string))


# def build_libfmt():
#     libfmt = join(PROJECT_ROOT, "libfmt")
#     if exists(join(libfmt, "libfmt.a")):
#         print("libfmt.a found")
#         return
#     if not exists(libfmt):
#         makedirs(libfmt)
#     src = "fmt-8.0.1"
#     src_path = join(PROJECT_ROOT, "vendor", f"{src}.tar.gz")
#     if not exists(src_path):
#         raise ValueError(f"{src_path} not found")
#     build = join(PROJECT_ROOT, "build_fmt")
#     if exists(build):
#         rmtree(build)
#     makedirs(build)
#     print("now:", os.getcwd())
#     orig_wd = os.getcwd()
#     os.chdir(build)
#     run(["tar", "xzf", src_path])
#     os.chdir(join(build, src))
#     run(["cmake", "-DCMAKE_POSITION_INDEPENDENT_CODE=TRUE", "."])
#     run(["make", "fmt"])
#     os.chdir(orig_wd)
#     copytree(join(build, src, "include", "fmt"), join(libfmt, "fmt"))
#     copy(join(build, src, "libfmt.a"), libfmt)
#
#
# build_libfmt()

readme = read("README.rst")
version = read_version()


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
            # "-pthread",
            # "-std=c++17",
            "-std=c++11",
            "-O3",
            "-Wno-unused-function",
            "-Wno-deprecated-declarations",
        ],
        # libraries=["fmt"],
        # include_dirs=["libfmt"],
        # library_dirs=["libfmt"],
    )


extensions = [
    pypyx_ext(NAME, "core", "common"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader_custom"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader_db"),
    pypyx_ext(NAME, "core", "mdx", "executor", "cube_loader"),
    pypyx_ext(NAME, "core", "mdx", "executor", "mdx_engine_lite"),
    pypyx_ext(NAME, "core", "mdx", "executor", "mdx_engine"),
]


setup(
    ext_modules=cythonize(
        extensions,
        language_level="3str",
        include_path=[
            # join(PROJECT_ROOT, NAME, "stdlib"),
            join(PROJECT_ROOT, NAME),
        ],
    ),
    name=NAME,
    version=version,
    description="OlaPy, an experimental OLAP engine based on Pandas",
    long_description=readme,
    python_requires="<4,>=3.8",
    project_urls={"homepage": "https://github.com/abilian/olapy"},
    author="Abilian SAS",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CythonPlus",
    ],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": ["olapy = olapy.__main__:cli", "etl = olapy.etl.etl:run_etl"]
    },
    packages=[
        "config",
        "cubes_templates",
        "demos.ETL",
        "micro_bench",
        "micro_bench.olapy_pandas_VS_olapy_pyspark",
        "olapy",
        "olapy.core",
        "olapy.core.mdx",
        "olapy.core.mdx.executor",
        "olapy.core.mdx.executor.spark",
        "olapy.core.mdx.parser",
        "olapy.core.mdx.tools",
        "olapy.core.services",
        "olapy.core.services.spark",
        "olapy.etl",
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
