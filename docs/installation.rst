.. _installation:

Installation
============

Install from PyPI
-----------------

You can install it directly from the `Python Package Index <https://pypi.python.org/pypi/olapy>`_::

    pip install olapy

Install from Github
-------------------

The project sources are stored in `Github repository <https://github.com/abilian/olapy>`_.

Download from Github::

    git clone git://github.com/abilian/olapy.git

Install::

    cd olapy
    python setup.py install


Testing
+++++++

OlaPy is configured perfectly to run units and integrations tests suites, you just need to use::

    tox

Olapy-data
----------

after installation, you can take a look to olapy-data folder located under::

    ~/olapy-data                       for linux/mac user

    C:\User\{USER_NAME}\olapy-data     for windows

This folder contains some required files to configure olapy and some demo cubes under /cubes folder, we will deeply discuss about this in the :advanced configuration section:

