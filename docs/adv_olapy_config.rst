
Advanced Olapy options
**********************

Olapy is easy configurable library,
with runserver command you can pass a lot of option to it:

    -h      : Host ip adresse.
    -p      : Host port.
    -wf     : Write logs into a file or display them into the console. log file location by default under olapy-data folder
    -lf     : If you want to change log file location.
    -sa     : SQL Alchemy URI to connect to database , **DON'T PUT THE DATABASE NAME !**
    -od     : Olapy-Data folder location
    -st     : Get cubes from where (db|csv), DEFAULT : csv only

here an example of olapy runserver with all options::

    olapy runserver -sa=postgresql://postgres:root@localhost:5432 -h=0.0.0.0 -p=8000 -wf=False -lf=/home/{USER_NAME}/Documents/olapy_logs -od=/home/{USER_NAME}/Documents -st=db,csv

