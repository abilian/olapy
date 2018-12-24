import os

import click

from olapy.etl.etl import run_etl


@click.command()
@click.pass_context
def main(ctx):
    # demo cli etl
    # after python setup.py install
    os.system(
        "etl --input_file_path=sales.xlsx --config_file=config.yml --output_cube_path='cube1'"
    )

    # demo run_etl as function with config as dict
    config = {
        "Facts": ["Amount", "Count"],
        "Geography": ["Continent", "Country", "City"],
        "Product": ["Company", "Article", "Licence"],
        "Date": ["Year", "Quarter", "Month", "Day"],
    }
    ctx.invoke(
        run_etl,
        input_file_path="sales.xlsx",
        cube_config=config,
        output_cube_path="cube2",
    )


if __name__ == "__main__":
    main()
