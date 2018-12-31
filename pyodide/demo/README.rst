Demo
----

install gunicorn with::

    pip install gunicorn


run::



    gunicorn -w 4 -b 0.0.0.0 -t 300 server:app


and go to http://localhost:8000/olapy.html
