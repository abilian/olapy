from __future__ import absolute_import, division, print_function

import itertools
from collections import OrderedDict

import numpy as np
import xmlwitch


class XmlaExecuteTools():
    """XmlaExecuteTools for generating xmla execute responses."""

    def __init__(self, executer):
        self.executer = executer

    @staticmethod
    def split_dataframe(mdx_execution_result):
        """
        Split DataFrame into multiple ones by dimension.
        
        example::

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


        :param mdx_execution_result: MdxEngine.execute_mdx() result
        :return: dict with multiple DataFrame
        """
        # TODO new version with facts as splited df maybe
        return OrderedDict(
            (key, mdx_execution_result['result'].reset_index()[list(value)])
            for key, value in mdx_execution_result['columns_desc']['all']
            .items())

    @staticmethod
    def get_tuple_without_nan(tuple):
        """
        Remove nan from tuple.

        example:

            in  : ['Geography','Continent','-1']
    
            out : ['Geography','Continent']

        :param tuple: tuple as list
        :return: tuple as list without -1

        """
        for att in tuple[::-1]:
            if att != -1:
                return tuple[:tuple.index(att) + 1]

        return tuple

    def generate_xs0_one_axis(self,
                              mdx_execution_result,
                              splited_df,
                              mdx_query_axis='all',
                              axis="Axis0"):
        """
        
        :param mdx_execution_result:
        :param splited_df:
        :return:
        """

        xml = xmlwitch.Builder()
        # only measure selected
        if mdx_execution_result['columns_desc'][mdx_query_axis].keys() == [
                self.executer.facts
        ]:
            if len(mdx_execution_result['columns_desc'][mdx_query_axis][
                    self.executer.facts]) == 1:
                # to ignore for tupls in itertools.chain(*tuples)
                tuples = []
            else:
                # ['Facts', 'Amount', 'Amount']
                tuples = [[[[self.executer.facts] + [mes] + [mes]]]
                          for mes in self.executer.selected_measures]
                first_att = 3

        # query with on columns and on rows (without measure)
        elif mdx_execution_result['columns_desc'][
                'columns'] and mdx_execution_result['columns_desc']['rows']:
            # ['Geography','America']
            tuples = [
                zip(*[[[key] + list(row)
                       for row in splited_df[key].itertuples(index=False)]
                      for key in splited_df.keys()
                      if key is not self.executer.facts])
            ]

            first_att = 2

        # query with on columns and on rows (many measures selected)
        else:
            # ['Geography','Amount','America']
            tuples = [
                zip(*[[[key] + [mes] + list(row)
                       for row in splited_df[key].itertuples(index=False)]
                      for key in splited_df.keys()
                      if key is not self.executer.facts])
                for mes in self.executer.selected_measures
            ]
            first_att = 3

        if tuples:
            with xml.Axis(name=axis):
                with xml.Tuples:
                    for tupls in itertools.chain(*tuples):
                        with xml.Tuple:
                            if tupls[0][1] in self.executer.measures and len(
                                    self.executer.selected_measures) > 1:
                                with xml.Member(Hierarchy="[Measures]"):
                                    xml.UName(
                                        '[Measures].[{0}]'.format(tupls[0][1]))
                                    xml.Caption('{0}'.format(tupls[0][1]))
                                    xml.LName('[Measures]')
                                    xml.LNum('0')
                                    xml.DisplayInfo('0')
                                    xml.HIERARCHY_UNIQUE_NAME('[Measures]')

                                if tupls[0][-1] in self.executer.measures:
                                    continue

                            for tupl in tupls:
                                tuple_without_minus_1 = self.get_tuple_without_nan(
                                    tupl)

                                # french caracteres
                                # TODO encode dataframe
                                if type(tuple_without_minus_1[-1]) == unicode:
                                    tuple_without_minus_1 = [
                                        x.encode('utf-8', 'replace')
                                        for x in tuple_without_minus_1
                                    ]

                                # todo ugly !!
                                with xml.Member(Hierarchy="[{0}].[{0}]".format(
                                        tuple_without_minus_1[0])):
                                    xml.UName('[{0}].[{0}].[{1}].{2}'.format(
                                        tuple_without_minus_1[0], splited_df[
                                            tuple_without_minus_1[0]].columns[
                                                len(tuple_without_minus_1) -
                                                first_att], '.'.join([
                                                    '[' + str(i) + ']'
                                                    for i in
                                                    tuple_without_minus_1[
                                                        first_att - 1:]
                                                ])))
                                    xml.Caption('{0}'.format(
                                        tuple_without_minus_1[-1]))
                                    xml.LName('[{0}].[{0}].[{1}]'.format(
                                        tuple_without_minus_1[0], splited_df[
                                            tuple_without_minus_1[0]].columns[
                                                len(tuple_without_minus_1) -
                                                first_att]))
                                    xml.LNum('{0}'.format(
                                        len(tuple_without_minus_1) -
                                        first_att))
                                    xml.DisplayInfo('131076')

                                    # PARENT_UNIQUE_NAME must be before HIERARCHY_UNIQUE_NAME (todo change it in xsd)
                                    if len(tuple_without_minus_1[first_att -
                                                                 1:]) > 1:
                                        xml.PARENT_UNIQUE_NAME(
                                            '[{0}].[{0}].[{1}].{2}'.format(
                                                tuple_without_minus_1[0],
                                                splited_df[
                                                    tuple_without_minus_1[0]]
                                                .columns[0], '.'.join([
                                                    '[' + str(i) + ']'
                                                    for i in
                                                    tuple_without_minus_1[
                                                        first_att - 1:-1]
                                                ])))

                                    xml.HIERARCHY_UNIQUE_NAME(
                                        '[{0}].[{0}]'.format(
                                            tuple_without_minus_1[0]))

        return str(xml)

    def generate_xs0(self, mdx_execution_result):
        """
        Example of xs0::


             <Axis name="Axis0">
                <Tuples>
                    <Tuple>
                        <Member Hierarchy="[Geography].[Geography]">
                            <UName>[Geography].[Geography].[Continent].[America]</UName>
                            <Caption>America</Caption>
                            <LName>[Geography].[Geography].[Continent]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>131076</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Geography].[Geography]</HIERARCHY_UNIQUE_NAME>
                        </Member>

                        <Member Hierarchy="[Product].[Product]">
                            <UName>[Product].[Product].[Company].[Crazy Development]</UName>
                            <Caption>Crazy Development</Caption>
                            <LName>[Product].[Product].[Company]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>131076</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Product].[Product]</HIERARCHY_UNIQUE_NAME>
                        </Member>
                        </Tuple>

                    <Tuple>
                        <Member Hierarchy="[Geography].[Geography]">
                            <UName>[Geography].[Geography].[Continent].[Europe]</UName>
                            <Caption>Europe</Caption>
                            <LName>[Geography].[Geography].[Continent]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>131076</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Geography].[Geography]</HIERARCHY_UNIQUE_NAME>
                        </Member>

                        <Member Hierarchy="[Product].[Product]">
                            <UName>[Product].[Product].[Company].[Crazy Development]</UName>
                            <Caption>Crazy Development</Caption>
                            <LName>[Product].[Product].[Company]</LName>
                            <LNum>0</LNum>
                            <DisplayInfo>131076</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Product].[Product]</HIERARCHY_UNIQUE_NAME>
                        </Member>
                    </Tuple>
                </Tuples>
             </Axis>

        :param mdx_execution_result: mdx_execute() result
        :return: xs0 xml as string
        """
        # TODO must be OPTIMIZED every time!!!!!

        dfs = self.split_dataframe(mdx_execution_result)
        if mdx_execution_result['columns_desc'][
                'rows'] and mdx_execution_result['columns_desc']['columns']:

            return """
            {0}
            {1}
            """.format(
                self.generate_xs0_one_axis(
                    mdx_execution_result,
                    dfs,
                    mdx_query_axis='columns',
                    axis="Axis0"),
                self.generate_xs0_one_axis(
                    mdx_execution_result,
                    dfs,
                    mdx_query_axis='rows',
                    axis="Axis1"))

        # only one measure selected
        elif not mdx_execution_result['columns_desc'][
                'rows'] and not mdx_execution_result['columns_desc']['columns'] and\
                mdx_execution_result['columns_desc']['where']:
            return """
            {0}
            """.format(
                self.generate_xs0_one_axis(
                    mdx_execution_result,
                    dfs,
                    mdx_query_axis='where',
                    axis="Axis0"))

        # one axis
        return self.generate_xs0_one_axis(
            mdx_execution_result, dfs, mdx_query_axis='columns', axis="Axis0")

    # TODO maybe fusion with generate xs0 for less iteration
    def generate_cell_data(self, mdx_execution_result):
        """
        Examle of CellData::

            <Cell CellOrdinal="0">
                <Value xsi:type="xsi:long">768</Value>
            </Cell>

            <Cell CellOrdinal="1">
                <Value xsi:type="xsi:long">255</Value>
            </Cell>

        :param mdx_execution_result: mdx_execute() result
        :return: CellData as string
        """

        if ((len(mdx_execution_result['columns_desc']['columns'].keys()) == 0)
                ^
            (len(mdx_execution_result['columns_desc']['rows'].keys()) == 0
             )) and self.executer.facts in mdx_execution_result[
                 'columns_desc']['all'].keys():

            # iterate DataFrame horizontally
            columns_loop = itertools.chain(*[
                mdx_execution_result['result'][measure]
                for measure in mdx_execution_result['result'].columns
            ])

        else:

            # iterate DataFrame vertically
            columns_loop = itertools.chain(
                *[
                    tuple
                    for tuple in mdx_execution_result['result'].itertuples(
                        index=False)
                ])

        xml = xmlwitch.Builder()
        index = 0
        for value in columns_loop:
            if np.isnan(value):
                value = ''
            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(str(value), **{'xsi:type': 'xsi:long'})

            index += 1
        return str(xml)

    def generate_axes_info_slicer(self, mdx_execution_result):
        """
        Not used dimensions.

        example AxisInfo::


            <AxesInfo>
                <AxisInfo name="SlicerAxis">
                    <HierarchyInfo name="[Time].[Time]">
                        <UName name="[Time].[Time].[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                        <Caption name="[Time].[Time].[MEMBER_CAPTION]" type="xs:string"/>
                        <LName name="[Time].[Time].[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                        <LNum name="[Time].[Time].[LEVEL_NUMBER]" type="xs:int"/>
                        <DisplayInfo name="[Time].[Time].[DISPLAY_INFO]" type="xs:unsignedInt"/>
                    </HierarchyInfo>
                    <HierarchyInfo name="[Measures]">
                        <UName name="[Measures].[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                        <Caption name="[Measures].[MEMBER_CAPTION]" type="xs:string"/>
                        <LName name="[Measures].[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                        <LNum name="[Measures].[LEVEL_NUMBER]" type="xs:int"/>
                        <DisplayInfo name="[Measures].[DISPLAY_INFO]" type="xs:unsignedInt"/>
                    </HierarchyInfo>
                </AxisInfo>
            </AxesInfo>

        :param mdx_execution_result: mdx_execute() result
        :return: AxisInfo as string
        """
        all_dimensions_names = self.executer.get_all_tables_names(
            ignore_fact=True)
        all_dimensions_names.append('Measures')

        xml = xmlwitch.Builder()

        slicer_list = list(
            set(all_dimensions_names) - set(
                table_name
                for table_name in mdx_execution_result['columns_desc']['all']))

        # we have to write measures after dimensions ! (todo change xsd)
        if 'Measures' in slicer_list:
            slicer_list.insert(
                len(slicer_list),
                slicer_list.pop(slicer_list.index('Measures')))

        if slicer_list:
            with xml.AxisInfo(name='SlicerAxis'):
                for dim_diff in slicer_list:
                    to_write = "[{0}].[{0}]".format(dim_diff)
                    if dim_diff == 'Measures':
                        # if measures > 1 we don't have to write measure
                        if self.executer.facts in mdx_execution_result[
                                'columns_desc']['all'] and len(
                                    mdx_execution_result['columns_desc'][
                                        'all'][self.executer.facts]) > 1:
                            continue
                        else:
                            to_write = "[Measures]"

                    with xml.HierarchyInfo(name=to_write):
                        xml.UName(
                            name="{0}.[MEMBER_UNIQUE_NAME]".format(to_write),
                            **{'type': 'xs:string'})
                        xml.Caption(
                            name="{0}.[MEMBER_CAPTION]".format(to_write),
                            **{'type': 'xs:string'})
                        xml.LName(
                            name="{0}.[LEVEL_UNIQUE_NAME]".format(to_write),
                            **{'type': 'xs:string'})
                        xml.LNum(
                            name="{0}.[LEVEL_NUMBER]".format(to_write),
                            **{'type': 'xs:int'})
                        xml.DisplayInfo(
                            name="{0}.[DISPLAY_INFO]".format(to_write),
                            **{'type': 'xs:unsignedInt'})

        return str(xml)

    def generate_one_axis_info(self,
                               mdx_execution_result,
                               mdx_query_axis='columns',
                               Axis='Axis0'):
        """
        Example AxisInfo::


            <AxesInfo>
                <AxisInfo name="Axis0">
                    <HierarchyInfo name="[Geography].[Geography]">
                        <UName name="[Geography].[Geography].[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                        <Caption name="[Geography].[Geography].[MEMBER_CAPTION]" type="xs:string"/>
                        <LName name="[Geography].[Geography].[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                        <LNum name="[Geography].[Geography].[LEVEL_NUMBER]" type="xs:int"/>
                        <DisplayInfo name="[Geography].[Geography].[DISPLAY_INFO]" type="xs:unsignedInt"/>
                        <PARENT_UNIQUE_NAME name="[Geography].[Geography].[PARENT_UNIQUE_NAME]" type="xs:string"/>
                        <HIERARCHY_UNIQUE_NAME name="[Geography].[Geography].[HIERARCHY_UNIQUE_NAME]" type="xs:string"/>
                    </HierarchyInfo>

                    <HierarchyInfo name="[Product].[Product]">
                        <UName name="[Product].[Product].[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                        <Caption name="[Product].[Product].[MEMBER_CAPTION]" type="xs:string"/>
                        <LName name="[Product].[Product].[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                        <LNum name="[Product].[Product].[LEVEL_NUMBER]" type="xs:int"/>
                        <DisplayInfo name="[Product].[Product].[DISPLAY_INFO]" type="xs:unsignedInt"/>
                        <PARENT_UNIQUE_NAME name="[Product].[Product].[PARENT_UNIQUE_NAME]" type="xs:string"/>
                        <HIERARCHY_UNIQUE_NAME name="[Product].[Product].[HIERARCHY_UNIQUE_NAME]" type="xs:string"/>
                    </HierarchyInfo>
                </AxisInfo>
            </AxesInfo>

        :param mdx_execution_result:
        :param mdx_query_axis:  columns or rows (columns by default)
        :param Axis: Axis0 or Axis1 (Axis0 by default)
        :return:
        """

        axis_tables = mdx_execution_result['columns_desc'][mdx_query_axis]
        xml = xmlwitch.Builder()
        # measure must be written at the top
        if axis_tables:
            with xml.AxisInfo(name=Axis):
                # many measures , then write this on the top
                if self.executer.facts in axis_tables.keys() and len(
                        axis_tables[self.executer.facts]) > 1:
                    with xml.HierarchyInfo(name='[Measures]'):
                        xml.UName(
                            name="[Measures].[MEMBER_UNIQUE_NAME]",
                            **{'type': 'xs:string'})
                        xml.Caption(
                            name="[Measures].[MEMBER_CAPTION]",
                            **{'type': 'xs:string'})
                        xml.LName(
                            name="[Measures].[LEVEL_UNIQUE_NAME]",
                            **{'type': 'xs:string'})
                        xml.LNum(
                            name="[Measures].[LEVEL_NUMBER]",
                            **{'type': 'xs:int'})
                        xml.DisplayInfo(
                            name="[Measures].[DISPLAY_INFO]",
                            **{'type': 'xs:unsignedInt'})
                        xml.PARENT_UNIQUE_NAME(
                            name="[Measures].[PARENT_UNIQUE_NAME]",
                            **{'type': 'xs:string'})
                        xml.HIERARCHY_UNIQUE_NAME(
                            name="[Measures].[HIERARCHY_UNIQUE_NAME]",
                            **{'type': 'xs:string'})

                for table_name in axis_tables:
                    if table_name != self.executer.facts:
                        with xml.HierarchyInfo(
                                name='[{0}].[{0}]'.format(table_name)):
                            xml.UName(
                                name="[{0}].[{0}].[MEMBER_UNIQUE_NAME]".format(
                                    table_name),
                                **{'type': 'xs:string'})
                            xml.Caption(
                                name="[{0}].[{0}].[MEMBER_CAPTION]".format(
                                    table_name),
                                **{'type': 'xs:string'})
                            xml.LName(
                                name="[{0}].[{0}].[LEVEL_UNIQUE_NAME]".format(
                                    table_name),
                                **{'type': 'xs:string'})
                            xml.LNum(
                                name="[{0}].[{0}].[LEVEL_NUMBER]".format(
                                    table_name),
                                **{'type': 'xs:int'})
                            xml.DisplayInfo(
                                name="[{0}].[{0}].[DISPLAY_INFO]".format(
                                    table_name),
                                **{'type': 'xs:unsignedInt'})
                            xml.PARENT_UNIQUE_NAME(
                                name="[{0}].[{0}].[PARENT_UNIQUE_NAME]".format(
                                    table_name),
                                **{'type': 'xs:string'})
                            xml.HIERARCHY_UNIQUE_NAME(
                                name="[{0}].[{0}].[HIERARCHY_UNIQUE_NAME]".
                                format(table_name),
                                **{'type': 'xs:string'})

        return str(xml)

    def generate_axes_info(self, mdx_execution_result):
        """
        
        :param mdx_execution_result: mdx_execute() result
        :return: AxisInfo as string
        """
        if mdx_execution_result['columns_desc']['rows']:
            return """
            {0}
            {1}
            """.format(
                self.generate_one_axis_info(
                    mdx_execution_result,
                    mdx_query_axis='columns',
                    Axis='Axis0'),
                self.generate_one_axis_info(
                    mdx_execution_result, mdx_query_axis='rows', Axis='Axis1'))

        return self.generate_one_axis_info(mdx_execution_result)

    @staticmethod
    def generate_cell_info():
        xml = xmlwitch.Builder()
        with xml.CellInfo:
            xml.Value(name="VALUE")
            xml.FormatString(name="FORMAT_STRING", **{'type': 'xs:string'})
            xml.Language(name="LANGUAGE", **{'type': 'xs:unsignedInt'})
            xml.BackColor(name="BACK_COLOR", **{'type': 'xs:unsignedInt'})
            xml.ForeColor(name="FORE_COLOR", **{'type': 'xs:unsignedInt'})
            xml.FontFlags(name="FONT_FLAGS", **{'type': 'xs:int'})

        return str(xml)

    def generate_slicer_axis(self, mdx_execution_result):
        """
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

        :param mdx_execution_result: mdx_execute() result
        :return: SlicerAxis as string
        """
        # not used dimensions
        unused_dimensions = list(
            set(self.executer.get_all_tables_names(ignore_fact=True)) - set(
                table_name
                for table_name in mdx_execution_result['columns_desc']['all']))
        xml = xmlwitch.Builder()
        if unused_dimensions:
            with xml.Axis(name="SlicerAxis"):
                with xml.Tuples:
                    with xml.Tuple:
                        for dim_diff in unused_dimensions:

                            # TODO encode dataframe
                            # french caracteres
                            if type(self.executer.tables_loaded[dim_diff].iloc[
                                    0][0]) == unicode:
                                column_attribut = self.executer.tables_loaded[
                                    dim_diff].iloc[0][0].encode('utf-8',
                                                                'replace')
                            else:
                                column_attribut = self.executer.tables_loaded[
                                    dim_diff].iloc[0][0]

                            with xml.Member(
                                    Hierarchy="[{0}].[{0}]".format(dim_diff)):
                                xml.UName('[{0}].[{0}].[{1}].[{2}]'.format(
                                    dim_diff, self.executer.tables_loaded[
                                        dim_diff].columns[0], column_attribut))
                                xml.Caption(str(column_attribut))
                                xml.LName('[{0}].[{0}].[{1}]'.format(
                                    dim_diff, self.executer.tables_loaded[
                                        dim_diff].columns[0]))
                                xml.LNum('0')
                                xml.DisplayInfo('2')

                        # if we have zero on one only measures used
                        if len(self.executer.selected_measures) <= 1:
                            with xml.Member(
                                    Hierarchy="[Measures]".format(dim_diff)):
                                xml.UName('[Measures].[{0}]'.format(
                                    self.executer.measures[0]))
                                xml.Caption(
                                    '{0}'.format(self.executer.measures[0]))
                                xml.LName('[Measures]')
                                xml.LNum('0')
                                xml.DisplayInfo('0')

        return str(xml)
