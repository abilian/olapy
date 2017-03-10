from __future__ import absolute_import, division, print_function


class selectStatement():

    def __init__(self, select_statement):
        self.select_statement = select_statement

    def __str__(self):
        return '{}'.format(self.select_statement)
