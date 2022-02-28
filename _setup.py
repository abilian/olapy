try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os.path

readme = ""
here = os.path.abspath(os.path.dirname(__file__))
readme_path = os.path.join(here, "README.rst")
if os.path.exists(readme_path):
    with open(readme_path, "rb") as stream:
        readme = stream.read().decode("utf8")

setup(
    long_description=readme,
    name="olapy",
    version="0.8.2",
    description="OlaPy, an experimental OLAP engine based on Pandas",
    python_requires="<4,>=3.8",
    project_urls={"homepage": "https://github.com/abilian/olapy"},
    author="Abilian SAS",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
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
    package_dir={"": "src"},
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
    extras_require={
        "dev": [
            "black",
            "devtools==0.*,>=0.8.0",
            "flake8",
            "flake8-bugbear",
            "flake8-comprehensions",
            "flake8-mutable",
            "flake8-pytest",
            "flake8-super-call",
            "flake8-tidy-imports",
            "gprof2dot",
            "ipdb",
            "isort==5.*,>=5.0.0",
            "mastool",
            "mccabe",
            "mypy",
            "pre-commit",
            "prettytable",
            "py-cpuinfo",
            "pylint",
            "pytest",
            "pytest-cov",
            "sphinx-click",
            "sphinx-rtd-theme",
            "tox",
            "types-pyyaml==6.*,>=6.0.1",
            "types-six==1.*,>=1.16.3",
        ],
        "etl": [
            "awesome-slugify",
            "bonobo",
            "bonobo-sqlalchemy<0.6.1",
            "python-dotenv",
            "whistle<1.0.1",
        ],
        "spark": ["pyspark<3"],
    },
)
