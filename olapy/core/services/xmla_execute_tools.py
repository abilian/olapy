from __future__ import absolute_import, division, print_function

import itertools
from collections import OrderedDict


class XmlaExecuteTools():
    """
    XmlaExecuteTools for generating xmla execute responses
    """

    def __init__(self, executer):
        self.executer = executer

    def split_DataFrame(self, mdx_execution_result):
        """
        Split DataFrame into multiple ones by dimension
        example:

        in :

        +-------------+----------+----------+---------+---------+
        | Continent   | Country  | Company  |Article  | Amount  |
        +=============+==========+==========+=========+=========+
        | America     | US       | MS       |Crazy De | 35150   |
        +-------------+----------+----------+---------+---------+

        out :

        { 'Geography':

        +-------------+----------+---------+
        | Continent   | Country  | Amount  |
        +=============+==========+=========+
        | America     | US       | 35150   |
        +-------------+----------+---------+
        ,

        'Product':

        +----------+---------+---------+
        | Company  |Article  | Amount  |
        +==========+=========+=========+
        | MS       |Crazy De | 35150   |
        +----------+---------+---------+

        }


        :param mdx_execution_result: MdxEngine.execute_mdx() result
        :return: dict with multiple DataFrame
        """

        # TODO new version with facts as splited df maybe
        return OrderedDict(
            (key, mdx_execution_result['result'].reset_index()[list(value)])
            for key, value in mdx_execution_result['columns_desc']['all']
            .items())

    def get_tuple_without_nan(self, tuple):
        """
        remove nan from tuple.
        example

        in :

        ['Geography','Continent','-1']

        out :

        ['Geography','Continent']

        :param tuple: tuple as list
        :return: tuple as list without -1

        """

        for index, att in enumerate(tuple[::-1]):
            if att != -1:
                return tuple[:len(tuple) - index]

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
        axis0 = ""
        # only measure selected
        if mdx_execution_result['columns_desc'][
                mdx_query_axis].keys() == [self.executer.facts]:
            if len(mdx_execution_result['columns_desc'][mdx_query_axis][
                    self.executer.facts]) == 1:
                # to ignore for tupls in itertools.chain(*tuples)
                tuples = []
            else:
                # ['Facts', 'Amount', 'Amount']
                tuples = [[[[self.executer.facts] + [mes] + [mes]]]
                          for mes in self.executer.measures]
                first_att = 3

        # query with on columns and on rows (without measure)
        elif mdx_execution_result['columns_desc'][
                'columns'] and mdx_execution_result['columns_desc']['rows']:

            # ['Geography','America']
            tuples = [
                zip(* [[[key] + list(row)
                        for row in splited_df[key].itertuples(index=False)]
                       for key in splited_df.keys()
                       if key is not self.executer.facts])
            ]

            first_att = 2

        # query with on columns and on rows (many measures selected)
        else:

            # ['Geography','Amount','America']
            tuples = [
                zip(* [[[key] + [mes] + list(row)
                        for row in splited_df[key].itertuples(index=False)]
                       for key in splited_df.keys()
                       if key is not self.executer.facts])
                for mes in self.executer.measures
            ]
            first_att = 3

        for tupls in itertools.chain(*tuples):
            axis0 += "<Tuple>\n"
            # [u'Geography', u'Amount', 'America']
            # tupls[0][1] --> Measure
            if tupls[0][1] in self.executer.measures and len(
                    self.executer.measures) > 1:

                axis0 += """
                <Member Hierarchy="[Measures]">
                    <UName>[Measures].[{0}]</UName>
                    <Caption>{0}</Caption>
                    <LName>[Measures]</LName>
                    <LNum>0</LNum>
                    <DisplayInfo>0</DisplayInfo>
                    <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                </Member>
                """.format(tupls[0][1])

                if len(tupls) == 1:
                    axis0 += "</Tuple>\n"
                    continue

            for tupl in tupls:
                tuple_without_minus_1 = self.get_tuple_without_nan(tupl)
                axis0 += """
                <Member Hierarchy="[{0}].[{0}]">
                    <UName>[{0}].[{0}].[{1}].{2}</UName>
                    <Caption>{3}</Caption>
                    <LName>[{0}].[{0}].[{1}]</LName>
                    <LNum>{4}</LNum>
                    <DisplayInfo>131076</DisplayInfo>""".format(
                    tuple_without_minus_1[0], splited_df[tuple_without_minus_1[
                        0]].columns[len(tuple_without_minus_1) - first_att],
                    '.'.join([
                        '[' + str(i) + ']'
                        for i in tuple_without_minus_1[first_att - 1:]
                    ]), tuple_without_minus_1[-1],
                    len(tuple_without_minus_1) - first_att)
                # PARENT_UNIQUE_NAME must be before HIERARCHY_UNIQUE_NAME
                if len(tuple_without_minus_1[first_att - 1:]) > 1:
                    axis0 += """
                    <PARENT_UNIQUE_NAME>[{0}].[{0}].[{1}].{2}</PARENT_UNIQUE_NAME>""".format(
                        tuple_without_minus_1[0],
                        splited_df[tuple_without_minus_1[0]].columns[0],
                        '.'.join([
                            '[' + str(i) + ']'
                            for i in tuple_without_minus_1[first_att - 1:-1]
                        ]))
                axis0 += """
                    <HIERARCHY_UNIQUE_NAME>[{0}].[{0}]</HIERARCHY_UNIQUE_NAME>
                </Member>
                """.format(tuple_without_minus_1[0])

            axis0 += "</Tuple>\n"

        if axis0:
            axis0 = """
            <Axis name="{0}">
                <Tuples>
                    {1}
                </Tuples>
            </Axis>
            """.format(axis, axis0)

        return axis0

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

        dfs = self.split_DataFrame(mdx_execution_result)
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
        examle of CellData::

            <Cell CellOrdinal="0">
                <Value xsi:type="xsi:long">768</Value>
            </Cell>

            <Cell CellOrdinal="1">
                <Value xsi:type="xsi:long">255</Value>
            </Cell>

        :param mdx_execution_result: mdx_execute() result
        :return: CellData as string
        """
        cell_data = ""
        index = 0
        for measure in mdx_execution_result['result'].columns.values:
            for value in mdx_execution_result['result'][measure]:
                cell_data += """
                <Cell CellOrdinal="{0}">
                    <Value xsi:type="xsi:long">{1}</Value>
                </Cell>
                """.format(index, value)
                index += 1
        return cell_data

    def generate_axes_info_slicer(self, mdx_execution_result):
        """
        Not used dimensions

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

        hierarchy_info_slicer = ""

        slicer_list = list(
            set(all_dimensions_names) - set([
                table_name
                for table_name in mdx_execution_result['columns_desc']['all']
            ]))

        # we have to write measures after dimensions !
        if 'Measures' in slicer_list:
            slicer_list.insert(
                len(slicer_list),
                slicer_list.pop(slicer_list.index('Measures')))

        for dim_diff in slicer_list:
            to_write = "[{0}].[{0}]".format(dim_diff)
            if dim_diff == 'Measures':
                # if measures > 1 we don't have to write measure
                if len(self.executer.measures) > 1:
                    continue
                else:
                    to_write = "[Measures]"
            hierarchy_info_slicer += """
                    <HierarchyInfo name="{0}">
                        <UName name="{0}.[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                        <Caption name="{0}.[MEMBER_CAPTION]" type="xs:string"/>
                        <LName name="{0}.[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                        <LNum name="{0}.[LEVEL_NUMBER]" type="xs:int"/>
                        <DisplayInfo name="{0}.[DISPLAY_INFO]" type="xs:unsignedInt"/>
                    </HierarchyInfo>
                    """.format(to_write)

        if hierarchy_info_slicer:
            hierarchy_info_slicer = "<AxisInfo name='SlicerAxis'>\n" + hierarchy_info_slicer + "\n</AxisInfo>\n"

        return hierarchy_info_slicer

    def generate_one_axis_info(self,
                               mdx_execution_result,
                               mdx_query_axis='columns',
                               Axis='Axis0'):
        """
        example AxisInfo::


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

        all_dimensions_names = self.executer.get_all_tables_names(
            ignore_fact=True)
        hierarchy_info = ""
        all_dimensions_names.append('Measures')
        for table_name in mdx_execution_result['columns_desc'][mdx_query_axis]:
            to_write = "[{0}].[{0}]".format(table_name)
            # measures must be added to axis0 if measures selected > 1
            if table_name == self.executer.facts and len(mdx_execution_result[
                    'columns_desc'][mdx_query_axis][table_name]) > 1:
                to_write = "[Measures]"
                all_dimensions_names.remove('Measures')
            elif table_name == self.executer.facts:
                continue

            hierarchy_info += """
                <HierarchyInfo name="{0}">
                    <UName name="{0}.[MEMBER_UNIQUE_NAME]" type="xs:string"/>
                    <Caption name="{0}.[MEMBER_CAPTION]" type="xs:string"/>
                    <LName name="{0}.[LEVEL_UNIQUE_NAME]" type="xs:string"/>
                    <LNum name="{0}.[LEVEL_NUMBER]" type="xs:int"/>
                    <DisplayInfo name="{0}.[DISPLAY_INFO]" type="xs:unsignedInt"/>
                    <PARENT_UNIQUE_NAME name="{0}.[PARENT_UNIQUE_NAME]" type="xs:string"/>
                    <HIERARCHY_UNIQUE_NAME name="{0}.[HIERARCHY_UNIQUE_NAME]" type="xs:string"/>
                </HierarchyInfo>
            """.format(to_write)
        if hierarchy_info:
            hierarchy_info = """
            <AxisInfo name='{0}'>
                {1}
            </AxisInfo>
            """.format(Axis, hierarchy_info)

        return hierarchy_info

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

    def generate_cell_info(self):
        return """
        <CellInfo>
            <Value name="VALUE"/>
            <FormatString name="FORMAT_STRING" type="xs:string"/>
            <Language name="LANGUAGE" type="xs:unsignedInt"/>
            <BackColor name="BACK_COLOR" type="xs:unsignedInt"/>
            <ForeColor name="FORE_COLOR" type="xs:unsignedInt"/>
            <FontFlags name="FONT_FLAGS" type="xs:int"/>
        </CellInfo>"""

    def generate_slicer_axis(self, mdx_execution_result):
        """
        example SlicerAxis::


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
        tuple = ""
        # not used dimensions
        for dim_diff in list(
                set(self.executer.get_all_tables_names(ignore_fact=True)) - set(
                    [
                        table_name
                        for table_name in mdx_execution_result['columns_desc'][
                            'all']
                    ])):
            tuple += """
            <Member Hierarchy="[{0}].[{0}]">
                <UName>[{0}].[{0}].[{1}].[{2}]</UName>
                <Caption>{2}</Caption>
                <LName>[{0}].[{0}].[{1}]</LName>
                <LNum>0</LNum>
                <DisplayInfo>2</DisplayInfo>
            </Member>
            """.format(dim_diff,
                       self.executer.tables_loaded[dim_diff].columns[0],
                       self.executer.tables_loaded[dim_diff].iloc[0][0])

        # if we have zero on one only measures used
        if len(self.executer.measures) <= 1:
            tuple += """
            <Member Hierarchy="[Measures]">
                <UName>[Measures].[{0}]</UName>
                <Caption>{0}</Caption>
                <LName>[Measures]</LName>
                <LNum>0</LNum>
                <DisplayInfo>0</DisplayInfo>
            </Member>
            """.format(self.executer.measures[0])

        if tuple:
            tuple = """
            <Axis name="SlicerAxis">
                <Tuples>
                    <Tuple>
                        {0}
                    </Tuple>
                </Tuples>
            </Axis>
            """.format(tuple)

        return tuple
