import os
from os.path import isdir, expanduser
from pathlib import Path

import click
import pandas as pd

import bonobo
import yaml
from bonobo.config import use


@use('input_file_path')
def extract_excel_pd(**kwargs):
    yield pd.read_excel(kwargs.get('input_file_path'))


@use('cube_config')
def transform(*args, **kwargs):
    # pas de transformation ligne par ligne
    df = args[0]  # args 0 is the df
    olapy_data_set = {}
    for table_name, columns in kwargs.get('cube_config').items():
        olapy_data_set[table_name] = df[columns].copy()
        olapy_data_set[table_name].reset_index()
        if table_name.upper() != 'FACTS':
            # add primary key (X_id) column to the table
            olapy_data_set[table_name][table_name.lower() + '_id'] = df.index
    yield olapy_data_set


@use('output_cube_path')
def load_to_olapy(*args, **kwargs):
    olapy_data_set = args[0]
    if not isdir(kwargs.get('output_cube_path')):
        os.mkdir(kwargs.get('output_cube_path'))
    for df_name, df in olapy_data_set.items():
        save_to = os.path.join(kwargs.get('output_cube_path'), df_name + '.csv')
        df.to_csv(path_or_buf=save_to, sep=';', encoding='utf8')


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(extract_excel_pd,
                    transform,
                    load_to_olapy
                    )

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
@click.option('--input_file_path', default=None, help='Input file')
@click.option('--cube_config', '-config', default=None, help='Configuration file path')
@click.option('--output_cube_path', '-out_cube', default=None, help='Cube export path')
def run_etl(input_file_path, cube_config, output_cube_path):
    parser = bonobo.get_argument_parser()
    parser.add_argument("--input_file_path", help="Input file")
    parser.add_argument("-config", "--cube_config", help="Configuration file path")
    parser.add_argument("-out_cube", "--output_cube_path", help="Cube export path")
    with bonobo.parse_args(parser) as options:

        if cube_config:
            with open(cube_config) as config_file:
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
            options['output_cube_path'] = os.path.join(expanduser('~'),
                                                       Path(input_file_path).stem)

        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    # python etl2.py --input_file_path='/home/moddoy/Downloads/Activity Export.Jan.Fev.2018.xlsx' -config='etl_conf.yml'
    run_etl()
