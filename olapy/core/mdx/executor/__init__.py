import logging

try:
    # if pyspark install, use it instead of pandas

    from .spark.cube_loader import SparkCubeLoader as CubeLoader
    from .spark.execute import SparkMdxEngine as MdxEngine

    logging.warning(' **************************************************** ')
    logging.warning(' ***************** OlaPy with Spark ***************** ')
    logging.warning(' **************************************************** ')
except ImportError:

    from .cube_loader import CubeLoader  # type: ignore
    from .execute import MdxEngine  # type: ignore

    logging.warning(' ***************************************************** ')
    logging.warning(' ***************** OlaPy with Pandas ***************** ')
    logging.warning(' ***************************************************** ')
