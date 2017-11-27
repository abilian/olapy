
Advanced Olapy options
**********************

Olapy is ileasy configurable. When using the ``olapy runserver`` command, you can pass a lot of options to it::

    -st     Get cubes from where (db|csv), DEFAULT : csv only
    -sa     SQL Alchemy URI to connect to database , **DON'T PUT THE DATABASE NAME !**
    -wf     Write logs into a file or display them into the console. log file location by default under olapy-data folder
    -lf     If you want to change log file location.
    -od     Olapy-Data folder location
    -h      Host ip adresse
    -p      Host port

Here is an example of ``olapy runserver`` with all options::

    olapy runserver -sa=postgresql://postgres:root@localhost:5432 \
            -wf=False -lf=/home/{USER_NAME}/Documents/olapy_logs \
            -od=/home/{USER_NAME}/Documents -st=db,csv -h=0.0.0.0 -p=8000
