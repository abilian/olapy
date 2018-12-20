## OlaPy 0.7, What’s New?

v0.7 (May 15, 2018)
This is a major release from 0.6 and includes a number of API changes, 
new features, enhancements, and performance improvements along 
with a large number of bug fixes.

###### Highlights include:

- [Optionally use PySpark (beta)](#opt_spark)
- [Bonobo ETL is now optional in installation](#opt_bonobo)
- [OlaPy can run in browser with Pyodide](#olapy_pyodide)
- Python 3.7 support
- Big Refactor and API cleanup
- Many bugs fixes

---

### New features

* <a name="opt_spark">Optionally use PySpark (beta)</a>

For its memory consumption, at a certain moment the use of pandas is no longer effective, 
so an alternative is to use [PySpark](https://spark.apache.org/docs/0.9.0/python-programming-guide.html) 
as data analysis tool, OlaPy can now support Spark as data analysis tools instead of default tool which is pandas.

to use Spark with OlaPy:

    pip install olapy[spark]
    
and to go back to pandas, just uninstall spark:

    pip uninstall pyspark


* <a name="opt_bonobo">[Bonobo ETL](https://www.bonobo-project.org/) is now optional in installation</a>

OlaPy v0.6 includes by default Bonobo ETL, starting from v0.7 Bonobo will be installed in option, to do so:

    pip install olapy[bonobo]

**Reminder**
*If you have a single excel file containing your data and you want to use olapy, you can use bonobo-etl 
[module](https://github.com/abilian/olapy/blob/master/olapy/etl/etl.py) included 
in olapy package to extract data from your excel file, make transformations, and load then into olapy data folder.*

* <a name="olapy_pyodide">OlaPy can run in browser with Pyodide</a>

OlaPy can run inside your web browser as library (not XMLA server), 
thus all computations happens in the browser, with direct access to Web API technologies like the DOM.
how could this happens? well, thanks to iodide and pyodide projects which feature Python running entirely in the browser, 
allowing portable and reproducible data science using Python without having to install anything.

For more information see [OlaPy-Pyodide](https://github.com/abilian/olapy/tree/master/pyodide)

---

You can check some projects built on top of olapy

[OlaPy-Web](https://github.com/abilian/olapy-web)

[OlaPy-Viz](https://github.com/abilian/olapy-web/tree/olapy-web2.0)
