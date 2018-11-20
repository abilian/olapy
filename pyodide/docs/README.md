## Olapy-Pyodide

The purpose of OlaPy-Pyodide package is to show you how to use [OlaPy](https://github.com/abilian/olapy) inside your web browser as library 
(not XMLA server), thus all computations happens in the browser, with direct access to Web API technologies like the DOM. 

Since there's no server side, Sharing a notebook become very easy as passing around a single HTML file.

how could this happens? well, thanks to [iodide](https://github.com/iodide-project/iodide) and [pyodide](https://github.com/iodide-project/pyodide) projects

![OlaPy-Pyodide](https://raw.githubusercontent.com/abilian/olapy/pyodide/docs/img/olapy-pyodide.gif)


Build OlaPy with Pyodide
------------------------

After building [pyodide project](https://github.com/iodide-project/pyodide)

copy the content of [under_packages](https://github.com/abilian/olapy/tree/master/pyodide/build_olapy_by_yourself/under_packages) and [under_build](https://github.com/abilian/olapy/tree/master/pyodide/build_olapy_by_yourself/under_build), under pyodide's build and packages folders

and copy [server.py](https://github.com/abilian/olapy/blob/master/pyodide/build_olapy_by_yourself/server.py) file to pyodide's root folder

next, in pyodide project run:

    cd packages
    make


finally to test a demo, you should install gunicorn:

    pip install gunicorn


 and then run the server from pyodide root folder with:

    gunicorn -D -w 4 -b 0.0.0.0 -t 300 server:app

and go to [http://localhost:8000/olapy.html](http://localhost:8000/olapy.html)
