# -*- encoding: utf8 -*-
"""
Parser for MDX queries, and Break it in parts.
"""

from __future__ import absolute_import, division, print_function, \
    unicode_literals

import regex

# flake8: noqa W605

# FIXME: make this regex more readable (split it)
REGEX = regex.compile(
    "(?u)(\[[(\u4e00-\u9fff)*\w+\d ]+\](\.\[[(\u4e00-\u9fff)*" +
    '\w+\d\.\,\s\(\)\_\-\:"\’\´\€\&\$ ' + "]+\])*\.?((Members)|(\[Q\d\]))?)")


class Parser(object):
    """
    Class for Parsing a MDX query
    """

    def __init__(self, mdx_query=None):
        self.mdx_query = mdx_query

    @staticmethod
    def split_tuple(tupl):
        """
        Split Tuple (String) into items.

        example::

             input : '[Geography].[Geography].[Continent].[Europe]'

             output : ['Geography','Geography','Continent','Europe']

        :param tupl: MDX Tuple as String
        :return: Tuple items in list
        """
        split_tupl = tupl.strip(" \t\n").split("].[")
        split_tupl[0] = split_tupl[0].replace("[", "")
        split_tupl[-1] = split_tupl[-1].replace("]", "")
        return split_tupl

    @staticmethod
    def get_tuples(query, start=None, stop=None):
        """Get all tuples in the mdx query.

        Example::


            SELECT  {
                        [Geography].[Geography].[All Continent].Members,
                        [Geography].[Geography].[Continent].[Europe]
                    } ON COLUMNS,

                    {
                        [Product].[Product].[Company]
                    } ON ROWS

                    FROM {sales}

        It returns ::

            [
                ['Geography','Geography','Continent'],
                ['Geography','Geography','Continent','Europe'],
                ['Product','Product','Company']
            ]


        :param query: mdx query
        :param start: keyword in the query where we start (examples start = SELECT)
        :param stop:  keyword in the query where we stop (examples start = ON ROWS)

        :return:  nested list of tuples (see the example)
        """
        if start is not None:
            start = query.index(start)
        if stop is not None:
            stop = query.index(stop)

        # clean the query (remove All, Members...)
        return [[
            tup_att.replace("All ", "").replace("[", "").replace("]", "")
            for tup_att in tup[0].replace(".Members", "").replace(
                ".MEMBERS",
                "",
            ).split("].[", )
            if tup_att
        ]
            for tup in REGEX.findall(query[start:stop])
            if len(tup[0].split("].[")) > 1]

    def decorticate_query(self, query):
        """Get all tuples that exists in the MDX Query by axes.

        Example::

            query : SELECT  {
                                [Geography].[Geography].[All Continent].Members,
                                [Geography].[Geography].[Continent].[Europe]
                            } ON COLUMNS,

                            {
                                [Product].[Product].[Company]
                            } ON ROWS

                            FROM {sales}

            output : {
                    'all': [   ['Geography', 'Geography', 'Continent'],
                               ['Geography', 'Geography', 'Continent', 'Europe'],
                               ['Product', 'Product', 'Company']],

                    'columns': [['Geography', 'Geography', 'Continent'],
                                ['Geography', 'Geography', 'Continent', 'Europe']],

                    'rows': [['Product', 'Product', 'Company']],
                    'where': []
                    }

        :param query: MDX Query
        :return: dict of axis as key and tuples as value
        """

        # Hierarchize -> ON COLUMNS , ON ROWS ...
        # without Hierarchize -> ON 0
        try:
            query = query.decode("utf-8")
        except:
            pass

        tuples_on_mdx_query = self.get_tuples(query)
        on_rows = []
        on_columns = []
        on_where = []

        try:
            # ON ROWS
            if "ON ROWS" in query:
                stop = "ON ROWS"
                if "ON COLUMNS" in query:
                    start = "ON COLUMNS"
                else:
                    start = "SELECT"
                on_rows = self.get_tuples(query, start, stop)

            # ON COLUMNS
            if "ON COLUMNS" in query:
                start = "SELECT"
                stop = "ON COLUMNS"
                on_columns = self.get_tuples(query, start, stop)

            # ON COLUMNS (AS 0)
            if "ON 0" in query:
                start = "SELECT"
                stop = "ON 0"
                on_columns = self.get_tuples(query, start, stop)

            # WHERE
            if "WHERE" in query:
                start = "FROM"
                on_where = self.get_tuples(query, start)

        except BaseException:  # pragma: no cover
            raise SyntaxError("Please check your MDX Query")

        return {
            "all": tuples_on_mdx_query,
            "columns": on_columns,
            "rows": on_rows,
            "where": on_where,
        }

    @staticmethod
    def add_tuple_brackets(tupl):
        """
        After splitting tuple with :func:`split_group`, you got some tuple like **aa].[bb].[cc].[dd**
        so add_tuple_brackets fix this by adding missed brackets **[aa].[bb].[cc].[dd]**.

        :param tupl: Tuple as string example  'aa].[bb].[cc].[dd'.
        :return: [aa].[bb].[cc].[dd] as string.
        """
        tupl = tupl.strip()
        if tupl[0] != "[":
            tupl = "[" + tupl
        if tupl[-1] != "]":
            tupl = tupl + "]"
        return tupl

    def split_group(self, group):
        """
        Split group of tuples.
        example::

            group : '[Geo].[Geo].[Continent],[Prod].[Prod].[Name],[Time].[Time].[Day]'

            out : ['[Geo].[Geo].[Continent]','[Prod].[Prod].[Name]','[Time].[Time].[Day]']

        :param group: Group of tuple as string.
        :return: Separated tuples as list.
        """
        split_group = group.replace("\n", "").replace("\t", "").split("],")
        return list(
            map(lambda tupl: self.add_tuple_brackets(tupl), split_group))

    def get_nested_select(self):
        """
        Get tuples groups in query like ::

                Select {
                    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 19,2010],
                    [Geography].[Geography].[Continent].[Europe],
                    [Measures].[Amount]),

                    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 17,2010],
                    [Geography].[Geography].[Continent].[Europe],
                    [Measures].[Amount])
                    }

                out :
                    ['[Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 19,2010],\
                    [Geography].[Geography].[Continent].[Europe],[Measures].[Amount]',

                    '[Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 17,2010],\
                    [Geography].[Geography].[Continent].[Europe],[Measures].[Amount]']

        :return: All groups as list of strings.

        """
        return regex.findall(r"\(([^()]+)\)", self.mdx_query)

    def hierarchized_tuples(self):
        # type: () -> bool
        """Check if `hierarchized <https://docs.microsoft.com/en-us/sql/mdx/hierarchize-mdx>`_  mdx query.

        :return: True | False
        """
        return "Hierarchize" in self.mdx_query
