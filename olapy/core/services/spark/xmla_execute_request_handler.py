# -*- encoding: utf8 -*-
"""
Managing all `EXECUTE <https://technet.microsoft.com/fr-fr/library/ms186691(v=sql.110).aspx>`_ requests and responses
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import itertools
from collections import OrderedDict

import xmlwitch

from ..xmla_execute_request_handler import XmlaExecuteReqHandler


class SparkXmlaExecuteReqHandler(XmlaExecuteReqHandler):
    """The Execute method executes XMLA commands provided in the Command element and returns any resulting data
    using the XMLA MDDataSet data type (for multidimensional result sets.)

    Example::

        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
           <Command>
              <Statement>
                 SELECT Hierarchize({[Measures].[amount]}) ON COLUMNS FROM [sales]
              </Statement>
           </Command>
           <Properties>
              <PropertyList>
                 <DataSourceInfo>Provider=MSOLAP;Data Source=local;</DataSourceInfo>
                 <Catalog>sales</Catalog>
                 <Format>Multidimensional</Format>
                 <AxisFormat>ClusterFormat</AxisFormat>
              </PropertyList>
           </Properties>
        </Execute>
    """

    def split_dataframe(self):
        """Split DataFrame into multiple ones by dimension.

        Example::

            in :

            +-------------+----------+----------+---------+---------+
            | Continent   | Country  | Company  |Article  | Amount  |
            +=============+==========+==========+=========+=========+
            | America     | US       | MS       |Crazy De | 35150   |
            +-------------+----------+----------+---------+---------+

            out :

            'Geography':

                +-------------+----------+---------+
                | Continent   | Country  | Amount  |
                +=============+==========+=========+
                | America     | US       | 35150   |
                +-------------+----------+---------+


            'Product':

                +----------+---------+---------+
                | Company  |Article  | Amount  |
                +==========+=========+=========+
                | MS       |Crazy De | 35150   |
                +----------+---------+---------+


        :return: dict with multiple DataFrame
        """
        # return OrderedDict((
        #     key,
        #     self.mdx_execution_result["result"].reset_index()[list(value)],
        # ) for key, value in self.columns_desc["all"].items())

        splitted_dataframes = OrderedDict()
        for table, columns in self.columns_desc["all"].items():

            if table.upper() == 'FACTS':
                columns = ['sum(' + col + ')' for col in columns]

            splitted_dataframes[table] = self.mdx_execution_result[
                "result"].select(list(columns))

        return splitted_dataframes

    def _generate_tuples_xs0(self, split_df, mdx_query_axis):

        first_att = None
        # in python 3 it returns odict_keys(['Facts']) instead of ['Facts']
        if list(self.columns_desc[mdx_query_axis].keys()) == [
                self.executor.facts,
        ]:
            if len(self.columns_desc[mdx_query_axis][
                    self.executor.facts]) == 1:
                # to ignore for tupls in itertools.chain(*tuples)
                tuples = []
            else:
                # ['Facts', 'Amount', 'Amount']
                tuples = [[[[self.executor.facts] + [mes] + [mes]]]
                          for mes in self.executor.selected_measures]
                first_att = 3

        # query with on columns and on rows (without measure)
        elif self.columns_desc["columns"] and self.columns_desc["rows"]:
            # Ex: ['Geography','America']
            tuples = [
                zip(*[[[key] + list(row)
                       for row in split_df[key].rdd.collect()]
                      for key in split_df.keys()
                      if key is not self.executor.facts]),
            ]

            first_att = 2

        # query with 'on columns' and 'on rows' (many measures selected)
        else:
            # Ex: ['Geography','Amount','America']

            tuples = [
                zip(*[[[key] + [mes] + list(row)
                       for row in split_df[key].rdd.collect()]
                      for key in split_df.keys()
                      if key is not self.executor.facts])
                for mes in self.executor.selected_measures
            ]
            first_att = 3

        return tuples, first_att

    def _generate_slicer_convert2formulas(self):
        """
        generate set et of hierarchies from which data is retrieved for a single member.
        For more information about the slicer axis, see
        `Specifying the Contents of a Slicer Axis (MDX) <https://docs.microsoft.com/en-us/sql/analysis-\
        services/multidimensional-models/mdx/mdx-query-and-slicer-axes-specify-the-contents-of-a-slicer-\
        axis?view=sql-server-2017>`_

        example::


            <Axis name="SlicerAxis">
                <Tuples>
                    <Tuple>
                        <Member Hierarchy="[Geography].[Geo]">
                            <UName>[Geography].[Geo].[All Regions]</UName>
                            <Caption>All Regions</Caption>
                            <LName>[Geography].[Geo].[All-Level]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>2</DisplayInfo>
                        </Member>
                        <Member Hierarchy="[Geography].[Economy]">
                            <UName>[Geography].[Economy].[All]</UName>
                            <Caption>All</Caption>
                            <LName>[Geography].[Economy].[All-Level]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>3</DisplayInfo>
                        </Member>
                        <Member Hierarchy="[Product].[Prod]">
                            <UName>[Product].[Prod].[Company].&amp;[Crazy Development ]</UName>
                            <Caption>Crazy Development </Caption>
                            <LName>[Product].[Prod].[Company]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>1</DisplayInfo>
                        </Member>
                        <Member Hierarchy="[Time].[Calendar]">
                            <UName>[Time].[Calendar].[Year].&amp;[2010]</UName>
                            <Caption>2010</Caption>
                            <LName>[Time].[Calendar].[Year]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>4</DisplayInfo>
                        </Member>
                    </Tuple>
                </Tuples>
            </Axis>

        :return:
        """
        xml = xmlwitch.Builder()
        with xml.Axis(name="SlicerAxis"):
            with xml.Tuples:
                with xml.Tuple:
                    for dim_diff in self.executor.get_all_tables_names(
                            ignore_fact=True, ):
                        column_attribut = self.executor.tables_loaded[
                            dim_diff].select(self.executor.tables_loaded[
                                dim_diff].columns[0]).first()[0]

                        with xml.Member(
                                Hierarchy="[{0}].[{0}]".format(dim_diff), ):
                            xml.UName("[{0}].[{0}].[{1}].[{2}]".format(
                                dim_diff,
                                self.executor.tables_loaded[dim_diff].columns[
                                    0],
                                column_attribut,
                            ), )
                            xml.Caption(str(column_attribut))
                            xml.LName("[{0}].[{0}].[{1}]".format(
                                dim_diff,
                                self.executor.tables_loaded[dim_diff].columns[
                                    0],
                            ), )
                            xml.LNum("0")
                            xml.DisplayInfo("2")

        return str(xml)

    def generate_cell_data(self):
        # # type: () -> text_type
        """
        Example of CellData::

            <Cell CellOrdinal="0">
                <Value xsi:type="xsi:long">768</Value>
            </Cell>

            <Cell CellOrdinal="1">
                <Value xsi:type="xsi:long">255</Value>
            </Cell>

        :return: CellData as string
        """

        if self.convert2formulas:
            return self._generate_cells_data_convert2formulas()
        measures_agg = [
            column for column in self.mdx_execution_result["result"].columns
            if 'sum(' in column
        ]
        if ((len(self.columns_desc["columns"].keys()) == 0
             or len(self.columns_desc["rows"].keys()) == 0)
                and self.executor.facts in self.columns_desc["all"].keys()):

            columns_loop = []
            for column in measures_agg:
                for row in self.mdx_execution_result["result"].select(
                        column).rdd.collect():
                    columns_loop.append(row[0])

        else:
            # iterate DataFrame vertically
            columns_loop = itertools.chain(*[
                column_value
                for column_value in self.mdx_execution_result["result"].select(
                    measures_agg).rdd.collect()
            ])

        xml = xmlwitch.Builder()
        index = 0
        for value in columns_loop:
            # if np.isnan(value):
            #     value = ""
            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(str(value), **{"xsi:type": "xsi:long"})

            index += 1

        return str(xml)

    def generate_slicer_axis(self):
        """
        Generate SlicerAxis which contains elements (dimensions) that are not used in the request

        Example SlicerAxis::

            <Axis name="SlicerAxis">
                <Tuples>
                    <Tuple>
                        <Member Hierarchy="[Time].[Time]">
                            <UName>[Time].[Time].[Year].[2010]</UName>
                            <Caption>2010</Caption>
                            <LName>[Time].[Time].Year]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>2</DisplayInfo>
                        </Member>
                        <Member Hierarchy="[Measures]">
                            <UName>[Measures].[Amount]</UName>
                            <Caption>Amount</Caption>
                            <LName>[Measures]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>0</DisplayInfo>
                        </Member>
                    </Tuple>
                </Tuples>
            </Axis>

        :return: SlicerAxis as string
        """
        # not used dimensions

        if self.convert2formulas:
            return self._generate_slicer_convert2formulas()

        unused_dimensions = sorted(
            list(set(self.executor.get_all_tables_names(ignore_fact=True)) - {table_name for table_name in
                                                                              self.columns_desc["all"]}, ), )
        xml = xmlwitch.Builder()
        if unused_dimensions:
            with xml.Axis(name="SlicerAxis"):
                with xml.Tuples:
                    with xml.Tuple:
                        for dim_diff in unused_dimensions:
                            column_attribut = self.executor.tables_loaded[
                                dim_diff].select(self.executor.tables_loaded[
                                    dim_diff].columns[0]).first()[0]
                            with xml.Member(
                                    Hierarchy="[{0}].[{0}]".format(dim_diff),
                            ):
                                xml.UName("[{0}].[{0}].[{1}].[{2}]".format(
                                    dim_diff,
                                    self.executor.tables_loaded[dim_diff]
                                    .columns[0],
                                    column_attribut,
                                ), )
                                xml.Caption(str(column_attribut))
                                xml.LName("[{0}].[{0}].[{1}]".format(
                                    dim_diff,
                                    self.executor.tables_loaded[dim_diff]
                                    .columns[0],
                                ), )
                                xml.LNum("0")
                                xml.DisplayInfo("2")

                        # Hierarchize
                        if (len(self.executor.selected_measures) <= 1 and (
                            self.executor.parser.hierarchized_tuples() or self.executor.facts in self.columns_desc[
                                "where"])):
                            with xml.Member(Hierarchy="[Measures]"):
                                xml.UName("[Measures].[{}]".format(
                                    self.executor.measures[0], ), )
                                xml.Caption("{}".format(
                                    self.executor.measures[0], ))
                                xml.LName("[Measures]")
                                xml.LNum("0")
                                xml.DisplayInfo("0")

        return str(xml)
