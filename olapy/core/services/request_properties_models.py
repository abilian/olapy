from __future__ import absolute_import, division, print_function, \
    unicode_literals


class Tuple(object):

    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Property():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Restriction():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Session():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Restrictionlist():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Propertieslist():

    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class Command():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class ExecuteRequest():

    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class DiscoverRequest():
    def __init__(self, **kwargs):
        self.__dict__ = kwargs
