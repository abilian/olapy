class Facts:

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)


class Dimension:

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)


class Cube:

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)
