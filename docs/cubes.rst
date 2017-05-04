.. _cubes:

Cubes creation
==============

If you want to add new cubes, this is very simple, just paste your csv files under a new folder with (its name will be your new cube name)
in cubes folder, so the path to your cube become, home-directory/olapy-data/cubes/YOUR_CUBE/{YOU_CSV_FILES}

**IMPORTANT**

Here are the rules to apply in the tables (csv files) so that it works perfectly:

1) make sure that your tables follow the `star schema <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
2) Fact table should be named 'Facts'
3) the id columns must have _id at the end (product_id, person_id...)
4) the columns name must be in a good order (hierarchy) (example : Continent -> Country -> City...)


*take a look to the default cubes structure (sales and foodmart)*

**THE SAME THING IF YOU WANT TO WORK WITH POSTGRES TABLES**

-----------------------------------------------------------------------

Here are two examples of dimensions structure:

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

*here we don't use id in tables*

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
