from timeit import Timer

from cube_generator import CUBE_NAME


class MicBench:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def bench(self, connection, query, cube=CUBE_NAME, number=1):
        """
         To be precise, this executes the query statement once, and
        then returns the time it takes to execute

        :param connection: connection object
        :param query: MDX query
        :param cube: cube name
        :param number: number of times through the loop, defaulting
        to one million
        :return: float execution time in seconds
        """
        return Timer(lambda: connection.Execute(query, Catalog=cube)).timeit(number=number)
