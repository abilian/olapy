.. _With connection string:
.. _With config file:

OlaPy ETL
*********

NOTE: this part is working only with Python 3.5+.

OlaPy ETL can be used if you have an excel file (one sheet) contains all your data in order to let OlaPy make necessary transformations relative to its rules on your data.

To use OlaPy ETL, after installing OlaPy with python setup.py install use the following command::

    etl --input_file_path=<EXCEL FILE PATH> --config_file=<CONFIG FILE PATH> [OPTIONAL] --output_cube_path=<PATH WHERE TO GENERATE THE CUBE>

config_file describe how to create the cube, here an example of the configuration file,
consider this excel sheet::

    Count	Continent   Country             Year    Month	        Day
    84	        America	    Canada	        2010	January 2010	January 1,2010
    841	        America	    Canada	        2010	January 2010	January 2,2010
    2	        America	    United States	2010	January 2010	January 3,2010

and we want to divide it into three table, we use a configuration file like this::

    Facts: [Count]
    Geography: [Continent, Country]
    Date: [Year, Month, Day]

and you save it as yaml file (.yml).
