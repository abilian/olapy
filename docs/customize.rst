.. _customize:

Cube customization
==================

If you don't want to follow olapy cubes rules and you want to customize your cube construction, you can use a configuration file, you can find the default example in ::

    ~/olapy-data/cubes/cubes-config.xml for mac/linux

    C:\\User\\{USER_NAME}\\olapy-data\\cubes\\cubes-config.xml for windows


Here is an examples of configuration:

Assuming we have two tables as follows under 'custom_cube' folder

table 1: stats (which is the facts table)

+----------------+---------+--------------------+----------------------+
| departement_id | amount  |    monthly_salary  |  total monthly cost  |
+----------------+---------+--------------------+----------------------+
|  111           |  1000   |      2000          |    3000              |
+----------------+---------+--------------------+----------------------+
| ...            |         |                    |                      |
+----------------+---------+--------------------+----------------------+

table 2: organization (which is a dimension)

+------+---------------+-----------+------------------+------------------+
| id   | type          |  name     |  acronym         | other colums.....|
+------+---------------+-----------+------------------+------------------+
|  111 | humanitarian  |  humania  | for better life  |                  |
+------+---------------+-----------+------------------+------------------+
| ...  | ...           |           |                  |                  |
+------+---------------+-----------+------------------+------------------+

you can use a configuration file like this to construct cube and access to it with excel::


    # if you want to set an authentication mechanism to access cube,
    # user must set a token with login url like 'http://127.0.0.1/admin
    # default password = admin
    xmla_authentication : False

    # cube name <==> db name
    name : custom_cube
    #csv | postgres | mysql ...
    source : csv

    # star building customized star schema
    facts :
      table_name : stats
      keys:
        departement_id : organization.id

      measures :
        # by default, all number type columns in facts table, or you can specify them here
        - amount
        - monthly_salary

    # star building customized dimensions display in excel from the star schema
    dimensions:
      #  IMPORTANT , put here facts table also
      - name : stats
        displayName : stats

      - name : organization
        displayName : Organization
        columns :
          - name : id
          - name : type
          - name : name
            column_new_name : full_name


