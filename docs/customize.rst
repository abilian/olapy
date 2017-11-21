.. _customize:

Cube customization
==================

If you don't want to follow olapy cubes rules and  you want to customize olapy cube construction, you can use a configuration file, you can find the default example under ::

    ~/olapy-data/cubes/cubes-config.xml for mac/linux

    C:\\User\\{USER_NAME}\\olapy-data\\cubes\\cubes-config.xml for windows


Here is an examples of configuration:

Assuming we have two tables as follows under 'labster' folder

table 1: stats_line (which is the facts table)

+----------------+---------+--------------------+----------------------+
| departement_id | amount  |    monthly_salary  |  total monthly cost  |
+----------------+---------+--------------------+----------------------+
|  111           |  1000   |      2000          |    3000              |
+----------------+---------+--------------------+----------------------+
| bla  bla bla   |         |                    |                      |
+----------------+---------+--------------------+----------------------+

table 2: orgunit (which is a dimension)

+------+---------------+-----------+------------------+------------------+
| id   | type          |  name     |  acronym         | other colums.....|
+------+---------------+-----------+------------------+------------------+
|  111 | humanitarian  |  humania  | for better life  |                  |
+------+---------------+-----------+------------------+------------------+
| bla  | bla   bla     |           |                  |                  |
+------+---------------+-----------+------------------+------------------+

you can use a configuration file like this to construct cube and access to it with excel::


    <?xml version="1.0" encoding="UTF-8"?>

        <cubes>

            <!-- if you want to set an authentication mechanism for excel to access cube,
                user must set a token with login url like 'http://127.0.0.1/admin  -->
            <!-- default password = admin -->

            <!-- enable/disable xmla authentication -->
            <xmla_authentication>False</xmla_authentication>

            <cube>

                <!-- cube name => csv folder name or database name -->
                <name>labster</name>

                <!-- source : csv | postgres | mysql | oracle | mssql -->
                <source>csv</source>


                <!-- star building customized star schema -->

                <facts>

                    <!-- facts table name -->
                    <table_name>stats_line</table_name>

                    <keys>
                        <!-- <column_name ref="[target_table_name].[target_column_name]">[Facts_column_name]</column_name> -->
                        <column_name ref="orgunit.id">departement_id</column_name>

                    </keys>

                    <!-- specify measures explicitly -->
                    <measures>

                        <!-- by default, all number type columns in facts table, or you can specify them here -->
                        <name>montant</name>
                        <name>salaire_brut_mensuel</name>
                        <name>cout_total_mensuel</name>
                    </measures>

                </facts>
                <!-- end building customized star schema -->


                <!-- star building customized dimensions display in excel from the star schema -->
                <dimensions>


                    <dimension>

                        <!-- if you want to keep the same name for excel display, just use the same name in name and displayName -->
                        <name>orgunit</name>
                        <displayName>Organisation</displayName>

                        <columns>

                            <!-- IMPORTANT !!!!  COLUMNS ORDER MATTER -->
                            <name>type</name>
                            <name>nom</name>
                            <name>sigle</name>
                        </columns>

                    </dimension>

                </dimensions>

                <!-- end building customized dimensions display in excel from the star schema -->


            </cube>

        </cubes>

