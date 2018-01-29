"""
Model of olapy database config file parser object
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import attr


@attr.s
class Facts(object):
    """Facts class used to encapsulate config file attributes."""

    table_name = attr.ib()
    keys = attr.ib()
    measures = attr.ib()


@attr.s
class Dimension(object):
    """Dimension class used to encapsulate config file attributes."""

    name = attr.ib()
    displayName = attr.ib()
    columns = attr.ib()


@attr.s
class Cube(object):
    """Cube class used to encapsulate config file attributes."""
    name = attr.ib()
    source = attr.ib()
    xmla_authentication = attr.ib()
    facts = attr.ib()
    dimensions = attr.ib()
