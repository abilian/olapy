import os

import pandas as pd

import bonobo

# import xlrd

TEMP_PATH = "/home/moddoy/Downloads/Activity Export.Jan.Fev.2018.xlsx"
TEMP_OLAPY_PATH = "/home/moddoy/olapy-data/cubes/trotinettes"
CONFIG = {
    'Facts': ['Price', 'Product or Service Default Purchase Price', 'Total Price', 'Quantity'],
    'Accounts': ['Source Account', 'Destination Account'],
    'Client': ['Client Activity', 'Client', 'Client Role', 'Client Function', 'Client Region', 'Client Province',
               'Client Address', 'Client Address City', 'Client Address ZipCode', 'Client Telephone'],
    'Date': ['Shipping Date', 'Delivery Date', 'Creation Date', 'Modification Date'],
    'Delivery': ['State', 'Delivery Type', 'Type', 'Owner', 'Delivery Title', 'Delivery Description',
                 'Delivery Reference'],
    'Product': ['Product Line', 'Product or Service Reference', 'Product or Service'],
    'Sales': ['Seller', 'Buyer', 'Reference'],
    'sender': ['Sender or Provider', 'Recipient or Beneficiary', 'Supplier', 'Invoice Sender'],
    'Trade': ['Trade Condition Type', 'Trade Condition', 'Trade Condition Reference']
}


# def extract_excel(file_path=TEMP_PATH, sheet_index=0):
#     wb = xlrd.open_workbook(file_path)
#     sheet = wb.sheet_by_index(sheet_index)
#     for row in sheet.get_rows():
#         yield row

def extract_excel_pd(file_path=TEMP_PATH):
    yield pd.read_excel(file_path)


def transform(*args):
    # pas de transformation ligne par ligne
    df = args[0]  # args 0 is the df
    olapy_data_set = {}
    for table_name, columns in CONFIG.items():
        olapy_data_set[table_name] = df[columns]
        olapy_data_set[table_name].reset_index()
        if table_name.upper() != 'FACTS':
            olapy_data_set[table_name][table_name.lower() + '_id'] = df.index
    yield olapy_data_set


def load_to_olapy(*args):
    olapy_data_set = args[0]
    for df_name, df in olapy_data_set.items():
        save_to = os.path.join(TEMP_OLAPY_PATH, df_name + '.csv')
        df.to_csv(path_or_buf=save_to, sep=';', encoding='utf8')


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    # graph.add_chain(extract, transform, load)
    graph.add_chain(extract_excel_pd,
                    # bonobo.Limit(10),
                    transform,
                    load_to_olapy
                    # bonobo.PrettyPrinter()
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
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
