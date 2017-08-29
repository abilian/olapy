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


@attr.s
class Table(object):
    """Column class used to encapsulate config file attributes for web client."""

    table_name = attr.ib()
    old_column_name = attr.ib()
    new_column_name = attr.ib()


@attr.s
class Dashboard(object):
    """Column class used to encapsulate config file attributes for web client."""

    table_name = attr.ib()
    old_column_name = attr.ib()
    new_column_name = attr.ib()
