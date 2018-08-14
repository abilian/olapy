import os

# import click
#
# from olapy.etl.etl2 import run_etl


def main():
    # after python setup.py install
    os.system("etl --input_file_path=sales.xlsx --config_file=config.yml --output_cube_path='./cube1'")

    # config = {
    #
    # }
    # todo this
    # run_etl(input_file_path='input_file.xls', cube_config=config, output_cube_path='./cube2')


if __name__ == '__main__':
    main()
