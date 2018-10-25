How to use this ?
-----------------

After building `pyodide project <https://github.com/iodide-project/pyodide>`_.

copy the content of **under_packages** and **under_build**, under pyodide's build and packages folders

and copy **server.py** to pyodide root folder

next, in pyodide project::

    cd packages
    make

- *if you are asked to put file to patch, them put the same asked file path*

finally to test a demo, you should install gunicorn::

    pip install gunicorn


 and then run the server from pyodide root folder with::

    gunicorn -D -w 4 -b 0.0.0.0 -t 300 server:app

and go to http://localhost:8000/olapy.html
