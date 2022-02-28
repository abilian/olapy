try:
    # if pyspark is installed, use it instead of pandas

    from .spark.cube_loader import SparkCubeLoader as CubeLoader
    from .spark.mdx_engine import SparkMdxEngine as MdxEngine

    print(
        """
    ****************************************************
    ***************** OlaPy with Spark *****************
    ****************************************************
    """
    )

except ImportError:

    from .cube_loader import CubeLoader  # type: ignore
    from .mdx_engine import MdxEngine  # type: ignore

    print(
        """
    ****************************************************
    ***************** OlaPy with Pandas ****************
    ****************************************************
    """
    )
