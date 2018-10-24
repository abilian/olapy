"""
If you have a single excel file containing your data and you want to use olapy, you should use this module to
extract data from your excel file, make transformations, and load then into olapy data folder.
"""
import os
from os.path import isdir, expanduser
from pathlib import Path

import click
import pandas as pd

import bonobo
import yaml
from bonobo.config import use
from pandas import DataFrame


@use('input_file_path')
def extract_excel_pd(**kwargs):
    # type: (dict) -> DataFrame
    """
    Bonobo's First chain, extract data from source.
    :return: Pandas DataFrame
    """
    yield pd.read_excel(kwargs.get('input_file_path'))


@use('cube_config')
def transform(*args, **kwargs):
    """
    Bonobo's second chain, transform data based on olapy rules.
    :return: dict of DataFrames
    """

    # pas de transformation ligne par ligne
    df = args[0]  # args 0 is the df
    olapy_data_set = {}
    for table_name, columns in kwargs.get('cube_config').items():
        olapy_data_set[table_name] = df[columns]
        olapy_data_set[table_name].index.names = ['_id']

    yield olapy_data_set


@use('output_cube_path')
def load_to_olapy(*args, **kwargs):
    """
    Bonobo's third chain, load data transformed to olapy-data.
    :return: generated tables into olapy data folder
    """
    olapy_data_set = args[0]
    if not isdir(kwargs.get('output_cube_path')):
        os.mkdir(kwargs.get('output_cube_path'))
    for df_name, df in olapy_data_set.items():
        save_to = os.path.join(
            kwargs.get('output_cube_path'), df_name + '.csv')
        df.to_csv(path_or_buf=save_to, sep=';', encoding='utf8')
    print('Loaded in ' + kwargs.get('output_cube_path'))


def get_graph(**options):
    # (Any) -> bonobo.Graph
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(extract_excel_pd, transform, load_to_olapy)

    return graph


def get_services(input_file_path, cube_config, output_cube_path, **options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {
        'output_cube_path': output_cube_path,
        'cube_config': cube_config,
        'input_file_path': input_file_path,
    }


@click.command()
@click.option('--input_file_path', '-in_file', default=None, help='Input file')
@click.option(
    '--config_file', '-config', default=None, help='Configuration file path')
@click.option(
    '--output_cube_path', '-out_cube', default=None, help='Cube export path')
def run_etl(input_file_path,
            config_file,
            output_cube_path=None,
            cube_config=None):
    """
    Run ETl Process for passed excel file.

    :param input_file_path: excel file path

    :param config_file: config file path

    example of config::

        # in the config file you specify for each table, columns associate with it.
        Facts: [Price, Quantity]
        Accounts: ['Source Account', 'Destination Account']
        Client: ['Client Activity', 'Client Role']

    :param output_cube_path: cube folder path

    :param cube_config: if you want to call run_etl as function, you can pass dict config directly as param,
    there an example::

        @click.command()
        @click.pass_context
        def myETL(ctx):
            # demo run_etl as function with config as dict
            config = {
                'Facts': ['Amount', 'Count'],
                'Geography': ['Continent', 'Country', 'City'],
                'Product': ['Company', 'Article', 'Licence'],
                'Date': ['Year', 'Quarter', 'Month', 'Day']
            }
            ctx.invoke(run_etl, input_file_path='sales.xlsx', cube_config=config, output_cube_path='cube2')

    """
    parser = bonobo.get_argument_parser()
    parser.add_argument('-in', "--input_file_path", help="Input file")
    parser.add_argument("-cf", "--config_file", help="Configuration file path")
    parser.add_argument("-out", "--output_cube_path", help="Cube export path")
    with bonobo.parse_args(parser) as options:

        if cube_config:
            options['cube_config'] = cube_config
        elif config_file:
            with open(config_file) as config_file:
                options['cube_config'] = yaml.load(config_file)
        else:
            raise Exception('Config file is not specified')

        if input_file_path:
            options['input_file_path'] = input_file_path
        else:
            raise Exception('Excel file is not specified')

        if output_cube_path:
            options['output_cube_path'] = output_cube_path
        else:
            options['output_cube_path'] = os.path.join(
                expanduser('~'), 'olapy-data', 'cubes',
                Path(input_file_path).stem)

        bonobo.run(get_graph(**options), services=get_services(**options))


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    run_etl()
