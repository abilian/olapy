.. _cubes:
.. _OLAPY CUBES RULES:

Cubes creation
==============

If you want to add new cube, this is very simple, put you csv files in a folder
(the folder name wil be the cube name), **make sure that they follow** :ref:`OLAPY CUBES RULES <OLAPY CUBES RULES>`,
and move them to `olapy-data/cubes` folder,
thus, the path to your cube will be like, `~/olapy-data/cubes/{YOUR_CUBE}/{YOU_CSV_FILES}` for mac/linux,
and `C:\\User\\{USER_NAME}\\olapy-data\\{YOUR_CUBE}\\{YOU_CSV_FILES}` for windows

OLAPY CUBES RULES
^^^^^^^^^^^^^^^^^

**NOTE : THE SAME THING IF YOU WANT TO WORK WITH DATABASES**

Here are the rules to apply to your tables so that can works perfectly with olapy:

1) make sure that your tables follow the `star schema <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
2) Fact table should be named 'Facts'
3) each table id columns, must be the same in facts table, example ( product_id column from product table must be product_id in Facts table,
4) avoid 'id' for id columns name, you should use something_id for example
5) the columns name must be in a good order (hierarchy) (example : Continent -> Country -> City...)


*take a look to the default cubes structure (sales and foodmart)*


-----------------------------------------------------------------------

Here are two examples of table structures that follows olapy rules:

Examples:
^^^^^^^^^

Cube 1
++++++


Geography table
---------------

+------------+------------+-----------+
| Geo_id     | Continent  | Country   |
+============+============+===========+
| 0001       | America    | Canada    |
+------------+------------+-----------+
|               bla    bla      bla   |
+------------+------------+-----------+
| 00526      | Europe     | France    |
+------------+------------+-----------+

Facts table
-----------

+------------+------------+-----------+-----------+
| Geo_id     | Prod_id    | Amount    | Count     |
+============+============+===========+===========+
| 0001       | 111111     | 5000      | 20        |
+------------+------------+-----------+-----------+
|               bla    bla      bla    bla        |
+------------+------------+-----------+-----------+
| 0011       |   222222   | 1000      | 40        |
+------------+------------+-----------+-----------+

Product table
-------------

+------------+------------+-----------+
| Prod_id    | Company    | Name      |
+============+============+===========+
| 111111     | Ferrero    | Nutella   |
+------------+------------+-----------+
|               bla    bla      bla   |
+------------+------------+-----------+
| 222222     |   Nestle   | KitKat    |
+------------+------------+-----------+



-------------------------------------------

Cube 2
++++++

*here we don't use id column name in tables*

Geography table
---------------

+-----------+------------+
| Continent  | Country   |
+============+===========+
| America    | Canada    |
+------------+-----------+
|    bla  bla   bla      |
+------------+-----------+
| Europe     | France    |
+------------+-----------+

Facts table
-----------

+------------+------------+-----------+-----------+
| Continent  | Company    | Amount    | Count     |
+============+============+===========+===========+
| America    | Ferrero    | 5000      | 20        |
+------------+------------+-----------+-----------+
|         bla    bla      bla    bla              |
+------------+------------+-----------+-----------+
| Europe     |   Nestle   | 1000      | 40        |
+------------+------------+-----------+-----------+

Product table
-------------

+------------+-----------+
| Company    | Name      |
+============+===========+
| Ferrero    | Nutella   |
+------------+-----------+
|     bla  bla  bla      |
+------------+-----------+
|   Nestle   | KitKat    |
+------------+-----------+
