Demo
----

Run the simple server with::

    python server.py

or with gunicorn ::

    pip install gunicorn

    gunicorn -w 4 server:app


and go to http://localhost:8000/olapy.html
