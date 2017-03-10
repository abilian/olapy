from __future__ import absolute_import, division, print_function

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
        DataFrames = OrderedDict()
        [DataFrames.update({key: mdx_execution_result['result'].reset_index()[list(value)]}) for key, value in
         mdx_execution_result['columns_desc'].items() if key != self.executer.facts]
        return DataFrames

    def get_tuple_without_nan(self, tuple):
        """
        remove nan from tuple
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

    def check_measures_only_selected(self, mdx_execution_result):
        """
        check if mdx query contains only measures


        :param mdx_execution_result: mdx_execute() result
        :return: True | False
        """
        return len(mdx_execution_result['columns_desc'].keys()) == 1 and mdx_execution_result['columns_desc'].keys()[
                                                                             0] == self.executer.facts

    def generate_xs0_measures_only(self, mdx_execution_result):
        """
        generate xs0 if only measures exists in the  mdx query

        :param mdx_execution_result: mdx_execute() result
        :return: xs0 xml as string
        """
        axis0 = ""
        if len(mdx_execution_result['columns_desc'][self.executer.facts]) > 1:
            for column in mdx_execution_result['result'].columns:
                axis0 += """
                <Tuple>
                    <Member Hierarchy="[Measures]">
                        <UName>[Measures].[{0}]</UName>
                        <Caption>{0}</Caption>
                        <LName>[Measures]</LName>
                        <LNum>0</LNum>
                        <DisplayInfo>0</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                    </Member>
                </Tuple>
                """.format(column)

        if axis0:
            axis0 = """
            <Axis name="Axis0">
                <Tuples>
                    {0}
                </Tuples>
            </Axis>
            """.format(axis0)

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
        # TODO must be OPTIMIZED every time !!!!!

        # only measures selected
        if self.check_measures_only_selected(mdx_execution_result):
            return self.generate_xs0_measures_only(mdx_execution_result)

        dfs = self.split_DataFrame(mdx_execution_result)
        axis0 = ""
        keys_without_fact = [key for key in mdx_execution_result['columns_desc'].keys() if
                             key != self.executer.facts]
        # every selected measure
        for mes in self.executer.measures:
            for row in zip(*([list(row) for row in dfs[key].itertuples(index=False)] for key in dfs.keys())):
                axis0 += "<Tuple>\n"
                if len(self.executer.measures) > 1:
                    axis0 += """
                    <Member Hierarchy="[Measures]">
                        <UName>[Measures].[{0}]</UName>
                        <Caption>{0}</Caption>
                        <LName>[Measures]</LName>
                        <LNum>0</LNum>
                        <DisplayInfo>0</DisplayInfo>
                        <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                    </Member>
                    """.format(mes)
                for index, tupl in enumerate(row):
                    tuple_without_minus_1 = self.get_tuple_without_nan(tupl)
                    # tuple without parent
                    axis0 += """
                        <Member Hierarchy="[{0}].[{0}]">
                            <UName>[{0}].[{0}].[{1}].{2}</UName>
                            <Caption>{3}</Caption>
                            <LName>[{0}].[{0}].[{1}]</LName>
                            <LNum>{4}</LNum>
                            <DisplayInfo>131076</DisplayInfo>""".format(
                        keys_without_fact[index],
                        dfs[keys_without_fact[index]].columns[len(tuple_without_minus_1) - 1],
                        '.'.join(['[' + str(i) + ']' for i in tuple_without_minus_1]),
                        tuple_without_minus_1[-1],
                        len(tuple_without_minus_1) - 1
                    )
                    # PARENT_UNIQUE_NAME must be before HIERARCHY_UNIQUE_NAME
                    if len(tuple_without_minus_1) > 1:
                        axis0 += """
                            <PARENT_UNIQUE_NAME>[{0}].[{0}].[{1}].{2}</PARENT_UNIQUE_NAME>""".format(
                            keys_without_fact[index],
                            dfs[keys_without_fact[index]].columns[0],
                            '.'.join(['[' + str(i) + ']' for i in tuple_without_minus_1[:-1]])
                        )
                    axis0 += """
                        <HIERARCHY_UNIQUE_NAME>[{0}].[{0}]</HIERARCHY_UNIQUE_NAME>
                        </Member>
                        """.format(keys_without_fact[index])

                axis0 += "</Tuple>\n"

        if axis0:
            axis0 = """
            <Axis name="Axis0">
                <Tuples>
                    {0}
                </Tuples>
            </Axis>
            """.format(axis0)

        return axis0

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

    def generate_axes_info(self, mdx_execution_result):
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
        # TODO reduce complexity
        all_dimensions_names = self.executer.get_all_tables_names(ignore_fact=True)
        hierarchy_info = ""
        all_dimensions_names.append('Measures')

        for table_name in mdx_execution_result['columns_desc']:
            to_write = "[{0}].[{0}]".format(table_name)
            # measures must be added to axis0 if measures selected > 1
            if table_name == self.executer.facts and len(mdx_execution_result['columns_desc'][table_name]) > 1:
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
            hierarchy_info = "<AxisInfo name='Axis0'>\n" + hierarchy_info + "\n</AxisInfo>\n"

        hierarchy_info_slicer = ""
        slicer_list = list(set(all_dimensions_names) - set(
            [table_name for table_name in mdx_execution_result['columns_desc']]))

        # we have to write measures after dimensions !
        if 'Measures' in slicer_list:
            slicer_list.insert(len(slicer_list), slicer_list.pop(slicer_list.index('Measures')))
        for dim_diff in slicer_list:
            to_write = "[{0}].[{0}]".format(dim_diff)
            if dim_diff == 'Measures':
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

        return hierarchy_info + '\n' + hierarchy_info_slicer

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
        for dim_diff in list(set(self.executer.get_all_tables_names(ignore_fact=True)) - set(
                [table_name for table_name in mdx_execution_result['columns_desc']])):
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
