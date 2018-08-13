import os

import pandas as pd

import bonobo
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


@use('olapy_data_path')
def load_to_olapy(*args, **kwargs):
    olapy_data_set = args[0]
    for df_name, df in olapy_data_set.items():
        save_to = os.path.join(kwargs.get('olapy_data_path'), df_name + '.csv')
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


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {
        'olapy_data_path': options.get('olapy_data_path'),
        'cube_config': options.get('cube_config'),
        'input_file_path': options.get('input_file_path'),
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        cube_config = {
            'Facts': ['Price', 'Product or Service Default Purchase Price', 'Total Price', 'Quantity'],
            'Accounts': ['Source Account', 'Destination Account'],
            'Client': ['Client Activity', 'Client', 'Client Role', 'Client Function', 'Client Region',
                       'Client Province',
                       'Client Address', 'Client Address City', 'Client Address ZipCode', 'Client Telephone'],
            'Date': ['Shipping Date', 'Delivery Date', 'Creation Date', 'Modification Date'],
            'Delivery': ['State', 'Delivery Type', 'Type', 'Owner', 'Delivery Title', 'Delivery Description',
                         'Delivery Reference'],
            'Product': ['Product Line', 'Product or Service Reference', 'Product or Service'],
            'Sales': ['Seller', 'Buyer', 'Reference'],
            'sender': ['Sender or Provider', 'Recipient or Beneficiary', 'Supplier', 'Invoice Sender'],
            'Trade': ['Trade Condition Type', 'Trade Condition', 'Trade Condition Reference']
        }
        options['cube_config'] = cube_config

        options['input_file_path'] = "/home/moddoy/Downloads/Activity Export.Jan.Fev.2018.xlsx"

        options['olapy_data_path'] = "/home/moddoy/olapy-data/cubes/trotinettes"
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
