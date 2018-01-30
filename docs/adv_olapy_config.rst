
Advanced Olapy options
**********************

Olapy is easy configurable. When using the ``olapy runserver`` command, you can pass a lot of options to it::

    -st     Get cubes from where (db|csv), DEFAULT : csv only
    -sa     SQL Alchemy URI to connect to database , **DON'T PUT THE DATABASE NAME !**
    -wf     Write logs into a file or display them into the console. log file location by default under olapy-data folder
    -lf     If you want to change log file location.
    -od     Olapy-Data folder location
    -h      Host ip adresse
    -p      Host port
    -dbc    Database configuration file path, Default : ~/olapy-data/olapy-config.yml
    -cbf    Cube config file path, default : ~/olapy-data/cube/cubes-config.yml


    -tf     File path or database table name if you want to construct cube from a single file (or table)
    -c      To explicitly specify columns if (construct cube from a single file), columns order matters
    -m      To explicitly specify measures if (construct cube from a single file)

Here is an example of ``olapy runserver`` with all options::

    olapy runserver -sa=postgresql://postgres:root@localhost:5432 \
            -wf=False -lf=/home/{USER_NAME}/Documents/olapy_logs \
            -od=/home/{USER_NAME}/Documents -st=db,csv -h=0.0.0.0 -p=8000


Here is an example of ``olapy runserver`` with a simple csv file::


        olapy runserver -tf=/home/moddoy/olapy-data/cubes/sales/Data.csv -c City,Licence,Amount,Count