How to use this ?
-----------------

After building `pyodide project <https://github.com/iodide-project/pyodide>`_.

copy the content of **under_packages** and **under_build**, under pyodide's build and packages folders

and copy **server.py** to pyodide root folder

next, in pyodide project::

    cd packages
    make

- *if you are asked to put file to patch, them put the same asked file path*

finally to test a demo, run the server from pyodide root folder with::

    python server.py

and go to http://localhost:8000/olapy.html
