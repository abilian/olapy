class Facts:
    """
    Facts class used to encapsulate config file attributes
    """

    def __init__(self, **kwargs):
        """
        :param kwargs: {table_name : 'something',
                        keys : 
                            {
                            column_name : 'something',
                            ref : 'something'
                            },
                        measures : 
                            { name : 'something' }
                        }

        """
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)


class Dimension:
    """
    Dimension class used to encapsulate config file attributes
    """

    def __init__(self, **kwargs):
        """
        
        :param kwargs: {
                        name : 'something',
                        displayName : 'something',
                        columns : 
                            { name : 'something' }
                        }
                        
        """
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)


class Cube:
    """
    Cube class used to encapsulate config file attributes
    """

    def __init__(self, **kwargs):
        """   
        :param kwargs: {
                        name : 'something',
                        source : 'something',
                        }
        """
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)
