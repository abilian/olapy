from __future__ import absolute_import, division, print_function

import logging
import os
from logging.handlers import RotatingFileHandler


class Logs:

    def __init__(self, file_name):
        self.file_name = file_name + ".log"
        self.root_path = self._create_log_file()

    def _create_log_file(self):
        path = os.path.abspath(__file__)
        path = path.lstrip(os.sep)
        root = path[:path.index(os.sep)]
        # TODO Fix this prob with c: (
        # http://stackoverflow.com/questions/2422798/python-os-path-join-on-windows)
        if not os.path.exists(os.path.join(root + '\\', 'logs')):
            os.makedirs(os.path.join(root + '\\', 'logs'))
        root = os.path.join(root + '\\', 'logs')
        return root

    def write_log(self, msg):
        # Creation of the logger object that will serve us to write in the logs
        logger = logging.getLogger()
        # We set the logger level to DEBUG, so it writes everything
        logger.setLevel(logging.DEBUG)

        # Creation of a formatter that will add time, level
        # Of each message when writing a message in the log
        formatter = logging.Formatter(
            '%(asctime)s :: %(levelname)s :: %(message)s')
        # Creation of a handler which will redirect message of the log to
        # file in append mode, with 1 backup and max size of 1MB

        file_handler = RotatingFileHandler(
            os.path.join(self.root_path, self.file_name), 'a', 1000000, 1)
        # We put the level on DEBUG, we tell him that he must use the formatter
        # Created earlier and add this handler to the logger
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Creation of a second handler that will redirect each log
        # On the console
        # Steam_handler = logging.StreamHandler ()
        # Steam_handler.setLevel (logging.DEBUG)
        # Logger.addHandler (steam_handler)

        # It's time to spam your code with logs everywhere:

        logger.info(msg)
        # logger.warning('Testing %s', 'foo')
