import logging

try:
    # if pyspark install, use it instead of pandas

    from .spark_cube_loader import SparkCubeLoader
    from .spark_engine import SparkMdxEngine

    MdxEngine = SparkMdxEngine
    CubeLoader = SparkCubeLoader

    logging.warning(' **************************************************** ')
    logging.warning(' ***************** OlaPy with Spark ***************** ')
    logging.warning(' **************************************************** ')
except ImportError:

    from .cube_loader import CubeLoader
    from .execute import MdxEngine

    logging.warning(' ***************************************************** ')
    logging.warning(' ***************** OlaPy with Pandas ***************** ')
    logging.warning(' ***************************************************** ')
