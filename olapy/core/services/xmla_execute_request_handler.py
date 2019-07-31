# -*- encoding: utf8 -*-
"""Managing all
`EXECUTE <https://technet.microsoft.com/fr-fr/library/ms186691(v=sql.110).aspx>`_
requests and responses."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import itertools
from datetime import datetime

import numpy as np
import xmlwitch
from typing import Text, List

from .dict_execute_request_handler import DictExecuteReqHandler
from .xmla_execute_xsds import execute_xsd


class XmlaExecuteReqHandler(DictExecuteReqHandler):
    """The Execute method executes XMLA commands provided in the Command
    element and returns any resulting data using the XMLA MDDataSet data type
    (for multidimensional result sets).

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

    def _gen_measures_xs0(self, xml, tupls):
        """add elements representing measures to axis 0 element.

        :param xml: axis 0 tu update, in xml format
        :param tupls: mdx query tuples
        """
        with xml.Member(Hierarchy="[Measures]"):
            xml.UName("[Measures].[{}]".format(tupls[0][1]))
            xml.Caption("{}".format(tupls[0][1]))
            xml.LName("[Measures]")
            xml.LNum("0")
            xml.DisplayInfo("0")
            if "HIERARCHY_UNIQUE_NAME" in self.mdx_query:
                xml.HIERARCHY_UNIQUE_NAME("[Measures]")

    def _gen_xs0_parent(self, xml, tuple, splitted_df, first_att):
        """For hierarchical rowset, we need to add the tuple ( mdx query
        contains PARENT_UNIQUE_NAME keyword)

        :param xml: axis 0 tu update, in xml format
        :param tuple: tuple that you want to add his parent
        :param splitted_df: spllited dataframes (with split_dataframe() function)
        :param first_att: tuple first attribut
        :return:
        """

        parent = ".".join(map(lambda x: "[" + str(x) + "]", tuple[first_att - 1 : -1]))
        if parent:
            parent = "." + parent
        xml.PARENT_UNIQUE_NAME(
            "[{0}].[{0}].[{1}]{2}".format(
                tuple[0], splitted_df[tuple[0]].columns[0], parent
            )
        )

    def _get_lvl_column_by_dimension(self, all_tuples):
        """TODO.

        :param all_tuples:
        :return:
        """
        used_levels = {}
        for tupl in all_tuples:
            if not tupl[0].upper() == "MEASURES":
                used_levels[tupl[0]] = list(used_levels.get(tupl[0], [])) + [tupl[2]]
        return used_levels

    def _gen_xs0_tuples(self, xml, tupls, **kwargs):
        first_att = kwargs.get("first_att")
        split_df = kwargs.get("split_df")
        all_tuples = self.executor.parser.decorticate_query(self.mdx_query)["all"]
        # [Geography].[Geography].[Continent]  -> first_lvlname : Country
        # [Geography].[Geography].[Europe]     -> first_lvlname : Europe
        all_level_columns = self._get_lvl_column_by_dimension(all_tuples)
        for idx, tupl in enumerate(tupls):
            tuple_without_minus_1 = self.get_tuple_without_nan(tupl)
            current_lvl_name = split_df[tuple_without_minus_1[0]].columns[
                len(tuple_without_minus_1) - first_att
            ]
            current_dimension = tuple_without_minus_1[0]
            if all(
                used_column in self.executor.tables_loaded[current_dimension].columns
                for used_column in all_level_columns[current_dimension]
            ):
                uname = "[{0}].[{0}].[{1}].{2}".format(
                    current_dimension,
                    current_lvl_name,
                    ".".join(
                        [
                            "[" + str(tuple_value) + "]"
                            for tuple_value in tuple_without_minus_1[first_att - 1 :]
                        ]
                    ),
                )
            else:
                uname = "[{0}].[{0}].{1}".format(
                    current_dimension,
                    ".".join(
                        [
                            "[" + str(tuple_value) + "]"
                            for tuple_value in tuple_without_minus_1[first_att - 1 :]
                        ]
                    ),
                )

            with xml.Member(Hierarchy="[{0}].[{0}]".format(tuple_without_minus_1[0])):
                xml.UName(uname)
                xml.Caption(str((tuple_without_minus_1[-1])))
                xml.LName(
                    "[{0}].[{0}].[{1}]".format(
                        tuple_without_minus_1[0], current_lvl_name
                    )
                )
                xml.LNum(str(len(tuple_without_minus_1) - first_att))
                xml.DisplayInfo("131076")

                if "PARENT_UNIQUE_NAME" in self.mdx_query.upper():
                    self._gen_xs0_parent(
                        xml,
                        tuple=tuple_without_minus_1,
                        splitted_df=split_df,
                        first_att=first_att,
                    )
                if "HIERARCHY_UNIQUE_NAME" in self.mdx_query.upper():
                    xml.HIERARCHY_UNIQUE_NAME(
                        "[{0}].[{0}]".format(tuple_without_minus_1[0])
                    )

    def tuples_2_xs0(self, tuples, splitted_df, first_att, axis):
        """transform mdx query tuples (list) to xmla xs0.

        :param tuples: list of tuples
        :param splitted_df: spllited dataframes (with split_dataframe() function)
        :param first_att: tuple first attribut
        :param axis: xs0 | xs1
        :return: tuples axis in xml
        """
        xml = xmlwitch.Builder()
        with xml.Axis(name=axis):
            with xml.Tuples:
                for tupls in itertools.chain(*tuples):
                    with xml.Tuple:
                        if (
                            tupls[0][1] in self.executor.measures
                            and len(self.executor.selected_measures) > 1
                        ):
                            self._gen_measures_xs0(xml, tupls)
                            if tupls[0][-1] in self.executor.measures:
                                continue
                        self._gen_xs0_tuples(
                            xml, tupls, split_df=splitted_df, first_att=first_att
                        )
                        # Hierarchize'
                        if not self.executor.parser.hierarchized_tuples():
                            self._gen_measures_xs0(xml, tupls)
        return xml

    def _gen_xs0_grouped_tuples(self, axis, tuples_groups):
        """generate xs0 axis form query with multiple data groups "exple:
        select (.. geography..)(..product..)(..time..)".

        :param axis: ax0 | ax1 ( depends on the mdx query axes )
        :param tuples_groups: list of tuples groups
        :return: tuples axis groups in xml
        """
        xml = xmlwitch.Builder()
        with xml.Axis(name=axis):
            with xml.Tuples:
                for group in tuples_groups:
                    with xml.Tuple:
                        for tupl in self.executor.parser.split_group(group):
                            split_tupl = self.executor.parser.split_tuple(tupl)
                            if split_tupl[0].upper() == "MEASURES":
                                hierarchy = "[Measures]"
                                l_name = "[" + split_tupl[0] + "]"
                                lvl = 0
                                displayinfo = "0"
                            else:
                                hierarchy = "[{0}].[{0}]".format(split_tupl[0])
                                l_name = "[{}]".format("].[".join(split_tupl[:3]))
                                lvl = len(split_tupl[4:])
                                displayinfo = "131076"

                            with xml.Member(Hierarchy=hierarchy):
                                xml.UName("{}".format(tupl.strip(" \t\n")))
                                xml.Caption("{}".format(split_tupl[-1]))
                                xml.LName(l_name)
                                xml.LNum(str(lvl))
                                xml.DisplayInfo(displayinfo)
        return str(xml)

    def generate_xs0_one_axis(self, splitted_df, mdx_query_axis="all", axis="Axis0"):
        """

        :param splitted_df: splitted dataframes (with split_dataframe() function)
        :param mdx_query_axis: axis to which you want generate xs0 xml, (rows, columns or all)
        :param axis: axis0 or axis1
        :return:
        """
        # patch 4 select (...) (...) (...) from bla bla bla
        if self.executor.check_nested_select():
            return self._gen_xs0_grouped_tuples(
                axis, self.executor.parser.get_nested_select()
            )

        xml = xmlwitch.Builder()
        tuples, first_att = self._generate_tuples_xs0(splitted_df, mdx_query_axis)
        if tuples:
            xml = self.tuples_2_xs0(tuples, splitted_df, first_att, axis)
        elif self.columns_desc["columns"].keys() == [self.executor.facts]:
            with xml.Axis(name=axis):
                with xml.Tuples:
                    with xml.Tuple:
                        with xml.Member(Hierarchy="[Measures]"):
                            xml.UName(
                                "[Measures].[{}]".format(
                                    self.executor.selected_measures[0]
                                )
                            )
                            xml.Caption(str(self.executor.selected_measures[0]))
                            xml.LName("[Measures]")
                            xml.LNum("0")
                            xml.DisplayInfo("0")
        return str(xml)

    def _generate_xs0_convert2formulas(self):
        """generate xs0 xml for convert formulas request.

        :return:
        """
        xml = xmlwitch.Builder()
        with xml.Axis(name="Axis0"):
            with xml.Tuples:
                if isinstance(self.mdx_execution_result, list):
                    for idx in range(len(self.mdx_execution_result) * 3):
                        with xml.Tuple:
                            with xml.Member(Hierarchy="[Measures]"):
                                xml.UName("[Measures].[{}]".format("XL_SD" + str(idx)))
                                xml.Caption("XL_SD" + str(idx))
                                xml.LName("[Measures]")
                                xml.LNum("0")
                                xml.DisplayInfo("0")
        return str(xml)

    def _generate_slicer_convert2formulas(self):
        """Generate set et of hierarchies from which data is retrieved for a
        single member.

        For more information about the slicer axis, see
        `Specifying the Contents of a Slicer Axis (MDX)
        <https://docs.microsoft.com/en-us/sql/analysis-services/multidimensional-models/mdx/mdx-query-and-slicer-axes-specify-the-contents-of-a-slicer-axis?view=sql-server-2017>`_

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
                        ignore_fact=True
                    ):
                        column_attribut = self.executor.tables_loaded[dim_diff].iloc[0][
                            0
                        ]
                        with xml.Member(Hierarchy="[{0}].[{0}]".format(dim_diff)):
                            xml.UName(
                                "[{0}].[{0}].[{1}].[{2}]".format(
                                    dim_diff,
                                    self.executor.tables_loaded[dim_diff].columns[0],
                                    column_attribut,
                                )
                            )
                            xml.Caption(str(column_attribut))
                            xml.LName(
                                "[{0}].[{0}].[{1}]".format(
                                    dim_diff,
                                    self.executor.tables_loaded[dim_diff].columns[0],
                                )
                            )
                            xml.LNum("0")
                            xml.DisplayInfo("2")

        return str(xml)

    def _generate_axes_convert2formulas(self):
        return self._generate_xs0_convert2formulas()

    def _generate_cells_data_convert2formulas(self):
        """generate cells data for convert formulas query.

        for each tuple: <Cell CellOrdinal="0">
        <Value>[Measures].[Amount]</Value> </Cell> <Cell
        CellOrdinal="1">     <Value>Amount</Value> </Cell> <Cell
        CellOrdinal="2">     <Value>[Measures]</Value> </Cell>
        """

        xml = xmlwitch.Builder()
        index = 0
        for tupl in self.mdx_execution_result:
            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(tupl)
            index += 1
            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(self.executor.parser.split_tuple(tupl)[-1])
            index += 1

            tupl2list = tupl.split(".")
            if tupl2list[0].upper() == "[MEASURES]":
                value = "[Measures]"
            else:
                value = "{0}.{0}.[{1}]".format(
                    tupl2list[0],
                    self.executor.tables_loaded[
                        tupl2list[0].replace("[", "").replace("]", "")
                    ].columns[len(tupl2list[4:])],
                )

            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(value)
            index += 1

        return str(xml)

    def generate_cell_data(self):
        # type: () -> Text
        """Returns Cell elements representing the cell data contained by a root
        element that uses the MDDataSet data type.

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

        if (
            len(self.columns_desc["columns"].keys()) == 0
            or len(self.columns_desc["rows"].keys()) == 0
        ) and self.executor.facts in self.columns_desc["all"].keys():
            # iterate DataFrame horizontally
            columns_loop = itertools.chain(
                *[
                    self.mdx_execution_result["result"][measure]
                    for measure in self.mdx_execution_result["result"].columns
                ]
            )

        else:
            # iterate DataFrame vertically
            columns_loop = itertools.chain(
                *[
                    tuple
                    for tuple in self.mdx_execution_result["result"].itertuples(
                        index=False
                    )
                ]
            )

        xml = xmlwitch.Builder()
        index = 0

        for value in columns_loop:
            if np.isnan(value):
                value = ""
            with xml.Cell(CellOrdinal=str(index)):
                xml.Value(str(value), **{"xsi:type": "xsi:long"})

            index += 1

        return str(xml)

    def _generate_axes_info_slicer_convert2formulas(self):
        """generate Slicer Axes for convert formulas query.

        :return:
        """
        xml = xmlwitch.Builder()
        with xml.AxisInfo(name="SlicerAxis"):
            for dim in self.executor.get_all_tables_names(ignore_fact=True):
                to_write = "[{0}].[{0}]".format(dim)
                with xml.HierarchyInfo(name=to_write):
                    xml.UName(name=to_write + ".[MEMBER_UNIQUE_NAME]", type="xs:string")
                    xml.Caption(name=to_write + ".[MEMBER_CAPTION]", type="xs:string")
                    xml.LName(name=to_write + ".[LEVEL_UNIQUE_NAME]", type="xs:string")
                    xml.LNum(name=to_write + ".[LEVEL_NUMBER]", type="xs:int")
                    xml.DisplayInfo(
                        name=to_write + ".[DISPLAY_INFO]", type="xs:unsignedInt"
                    )

        return str(xml)

    def generate_axes_info_slicer(self):
        """generates slicer axis which filters the data returned by the MDX
        SELECT statement, restricting the returned data so that only data
        intersecting with the specified members will be returned (Not used
        dimensions)

        Example AxisInfo::

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

        :return: AxisInfo as string
        """

        if self.convert2formulas:
            return self._generate_axes_info_slicer_convert2formulas()

        all_dimensions_names = self.executor.get_all_tables_names(ignore_fact=True)
        all_dimensions_names.append("Measures")

        xml = xmlwitch.Builder()

        slicer_list = sorted(
            list(
                set(all_dimensions_names)
                - {table_name for table_name in self.columns_desc["all"]}
            )
        )

        if "Measures" in slicer_list:
            slicer_list.insert(
                len(slicer_list), slicer_list.pop(slicer_list.index("Measures"))
            )

        if slicer_list:
            with xml.AxisInfo(name="SlicerAxis"):
                for dim_diff in slicer_list:
                    to_write = "[{0}].[{0}]".format(dim_diff)
                    if dim_diff == "Measures":

                        # if measures > 1 we don't have to write measure
                        # Hierarchize
                        if (
                            self.executor.facts in self.columns_desc["all"]
                            and (len(self.columns_desc["all"][self.executor.facts]) > 1)
                            or (
                                not self.executor.parser.hierarchized_tuples()
                                and not self.columns_desc["where"]
                            )
                        ):
                            continue

                        else:
                            to_write = "[Measures]"

                    with xml.HierarchyInfo(name=to_write):
                        xml.UName(
                            name=to_write + ".[MEMBER_UNIQUE_NAME]", type="xs:string"
                        )
                        xml.Caption(
                            name=to_write + ".[MEMBER_CAPTION]", type="xs:string"
                        )
                        xml.LName(
                            name=to_write + ".[LEVEL_UNIQUE_NAME]", type="xs:string"
                        )
                        xml.LNum(name=to_write + ".[LEVEL_NUMBER]", type="xs:int")
                        xml.DisplayInfo(
                            name=to_write + ".[DISPLAY_INFO]", type="xs:unsignedInt"
                        )

        return str(xml)

    def _gen_measures_one_axis_info(self, xml):
        """Add AxisInfo elements, representing the axis metadata contained by
        the parent OlapInfo element for measures to the passed xml structure.

        :param xml: xml structure to update
        """
        with xml.HierarchyInfo(name="[Measures]"):
            xml.UName(name="[Measures].[MEMBER_UNIQUE_NAME]", type="xs:string")
            xml.Caption(name="[Measures].[MEMBER_CAPTION]", type="xs:string")
            xml.LName(name="[Measures].[LEVEL_UNIQUE_NAME]", type="xs:string")
            xml.LNum(name="[Measures].[LEVEL_NUMBER]", type="xs:int")
            xml.DisplayInfo(name="[Measures].[DISPLAY_INFO]", type="xs:unsignedInt")
            if "PARENT_UNIQUE_NAME" in self.mdx_query:
                xml.PARENT_UNIQUE_NAME(
                    name="[Measures].[PARENT_UNIQUE_NAME]", type="xs:string"
                )
            if "HIERARCHY_UNIQUE_NAME" in self.mdx_query:
                xml.HIERARCHY_UNIQUE_NAME(
                    name="[Measures].[HIERARCHY_UNIQUE_NAME]", type="xs:string"
                )
        return xml

    def _generate_table_axis_info(self, xml, dimensions_names):
        # type: (xmlwitch.Builder, List[Text]) -> None
        """Add AxisInfo elements, representing the axis metadata contained by
        the parent OlapInfo element for Dimension to the passed xml structure.

        :param xml: xml structure to update (mutable)
        :param dimensions_names: dimension names (without facts table)
        """
        for dimension_name in dimensions_names:
            with xml.HierarchyInfo(name="[{0}].[{0}]".format(dimension_name)):
                xml.UName(
                    name="[{0}].[{0}].[MEMBER_UNIQUE_NAME]".format(dimension_name),
                    type="xs:string",
                )
                xml.Caption(
                    name="[{0}].[{0}].[MEMBER_CAPTION]".format(dimension_name),
                    type="xs:string",
                )
                xml.LName(
                    name="[{0}].[{0}].[LEVEL_UNIQUE_NAME]".format(dimension_name),
                    type="xs:string",
                )
                xml.LNum(
                    name="[{0}].[{0}].[LEVEL_NUMBER]".format(dimension_name),
                    type="xs:int",
                )
                xml.DisplayInfo(
                    name="[{0}].[{0}].[DISPLAY_INFO]".format(dimension_name),
                    type="xs:unsignedInt",
                )

                if "Hierarchize" in self.mdx_query:
                    xml.PARENT_UNIQUE_NAME(
                        name="[{0}].[{0}].[PARENT_UNIQUE_NAME]".format(dimension_name),
                        type="xs:string",
                    )
                    xml.HIERARCHY_UNIQUE_NAME(
                        name="[{0}].[{0}].[HIERARCHY_UNIQUE_NAME]".format(
                            dimension_name
                        ),
                        type="xs:string",
                    )

    def generate_one_axis_info(self, mdx_query_axis="columns", Axis="Axis0"):
        """AxisInfo elements for one axis (axis0 or axis1).

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

        :param mdx_query_axis:  columns or rows (columns by default)
        :param Axis: Axis0 or Axis1 (Axis0 by default)
        :return:
        """
        # Hierarchize !!
        axis_tables = self.columns_desc[mdx_query_axis]
        axis_tables_without_facts = [
            table_name
            for table_name in axis_tables
            if table_name != self.executor.facts
        ]
        xml = xmlwitch.Builder()
        # measure must be written at the top
        if axis_tables:
            with xml.AxisInfo(name=Axis):
                # many measures , then write this on the top
                if (
                    self.executor.facts in axis_tables.keys()
                    and len(axis_tables[self.executor.facts]) > 1
                ):
                    self._gen_measures_one_axis_info(xml)
                self._generate_table_axis_info(xml, axis_tables_without_facts)
                # Hierarchize
                if (
                    not self.executor.parser.hierarchized_tuples()
                    and len(
                        self.columns_desc["columns"].get(self.executor.facts, [1, 1])
                    )
                    == 1
                ):
                    self._gen_measures_one_axis_info(xml)

        return str(xml)

    def _generate_axes_info_convert2formulas(self):
        """AxisInfo elements when convert to formulas,

        xml structure always Axis0 with measures and SlicerAxis with all dimensions like this::

            <AxisInfo name="Axis0">
                <HierarchyInfo name="[Measures]">
                    <UName name="[Measures].[MEMBER_UNIQUE_NAME]" type="xsd:string"/>
                    <Caption name="[Measures].[MEMBER_CAPTION]" type="xsd:string"/>
                    <LName name="[Measures].[LEVEL_UNIQUE_NAME]" type="xsd:string"/>
                    <LNum name="[Measures].[LEVEL_NUMBER]" type="xsd:int"/>
                    <DisplayInfo name="[Measures].[DISPLAY_INFO]" type="xsd:unsignedInt"/>
                </HierarchyInfo>
            </AxisInfo>
            <AxisInfo name="SlicerAxis">
                <HierarchyInfo name="[Geography].[Geo]">
                    <UName name="[Geography].[Geo].[MEMBER_UNIQUE_NAME]" type="xsd:string"/>
                    <Caption name="[Geography].[Geo].[MEMBER_CAPTION]" type="xsd:string"/>
                    <LName name="[Geography].[Geo].[LEVEL_UNIQUE_NAME]" type="xsd:string"/>
                    <LNum name="[Geography].[Geo].[LEVEL_NUMBER]" type="xsd:int"/>
                    <DisplayInfo name="[Geography].[Geo].[DISPLAY_INFO]" type="xsd:unsignedInt"/>
                </HierarchyInfo>
                <HierarchyInfo name="[Geography].[Economy]">
                    <UName name="[Geography].[Economy].[MEMBER_UNIQUE_NAME]" type="xsd:string"/>
                    <Caption name="[Geography].[Economy].[MEMBER_CAPTION]" type="xsd:string"/>
                    <LName name="[Geography].[Economy].[LEVEL_UNIQUE_NAME]" type="xsd:string"/>
                    <LNum name="[Geography].[Economy].[LEVEL_NUMBER]" type="xsd:int"/>
                    <DisplayInfo name="[Geography].[Economy].[DISPLAY_INFO]" type="xsd:unsignedInt"/>
                </HierarchyInfo>
                <HierarchyInfo name="[Product].[Prod]">
                    <UName name="[Product].[Prod].[MEMBER_UNIQUE_NAME]" type="xsd:string"/>
                    <Caption name="[Product].[Prod].[MEMBER_CAPTION]" type="xsd:string"/>
                    <LName name="[Product].[Prod].[LEVEL_UNIQUE_NAME]" type="xsd:string"/>
                    <LNum name="[Product].[Prod].[LEVEL_NUMBER]" type="xsd:int"/>
                    <DisplayInfo name="[Product].[Prod].[DISPLAY_INFO]" type="xsd:unsignedInt"/>
                </HierarchyInfo>
                <HierarchyInfo name="[Time].[Calendar]">
                    <UName name="[Time].[Calendar].[MEMBER_UNIQUE_NAME]" type="xsd:string"/>
                    <Caption name="[Time].[Calendar].[MEMBER_CAPTION]" type="xsd:string"/>
                    <LName name="[Time].[Calendar].[LEVEL_UNIQUE_NAME]" type="xsd:string"/>
                    <LNum name="[Time].[Calendar].[LEVEL_NUMBER]" type="xsd:int"/>
                    <DisplayInfo name="[Time].[Calendar].[DISPLAY_INFO]" type="xsd:unsignedInt"/>
                </HierarchyInfo>
            </AxisInfo>
        """
        xml = xmlwitch.Builder()
        with xml.AxisInfo(name="Axis0"):
            # many measures , then write this on the top
            with xml.HierarchyInfo(name="[Measures]"):
                xml.UName(name="[Measures].[MEMBER_UNIQUE_NAME]", type="xs:string")
                xml.Caption(name="[Measures].[MEMBER_CAPTION]", type="xs:string")
                xml.LName(name="[Measures].[LEVEL_UNIQUE_NAME]", type="xs:string")
                xml.LNum(name="[Measures].[LEVEL_NUMBER]", type="xs:int")
                xml.DisplayInfo(name="[Measures].[DISPLAY_INFO]", type="xs:unsignedInt")

        return str(xml)

    def _generate_cell_info_convert2formuls(self):
        """CellInfo representation for convert to formulas.

        :return: CellInfo structure as string
        """
        xml = xmlwitch.Builder()
        with xml.CellInfo:
            xml.Value(name="VALUE")

        return str(xml)

    def generate_cell_info(self):
        """Generate CellInfo which represents the cell metadata contained by
        the parent OlapInfo element.

        :return: CellInfo structure as string
        """

        if self.convert2formulas:
            return self._generate_cell_info_convert2formuls()

        xml = xmlwitch.Builder()
        with xml.CellInfo:
            xml.Value(name="VALUE")
            xml.FormatString(name="FORMAT_STRING", type="xs:string")
            xml.Language(name="LANGUAGE", type="xs:unsignedInt")
            xml.BackColor(name="BACK_COLOR", type="xs:unsignedInt")
            xml.ForeColor(name="FORE_COLOR", type="xs:unsignedInt")
            xml.FontFlags(name="FONT_FLAGS", type="xs:int")

        return str(xml)

    def generate_slicer_axis(self):
        """Generate SlicerAxis which contains elements (dimensions) that are
        not used in the request.

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
            list(
                set(self.executor.get_all_tables_names(ignore_fact=True))
                - {table_name for table_name in self.columns_desc["all"]}
            )
        )
        xml = xmlwitch.Builder()
        if unused_dimensions:
            with xml.Axis(name="SlicerAxis"):
                with xml.Tuples:
                    with xml.Tuple:
                        for dim_diff in unused_dimensions:
                            column_attribut = self.executor.tables_loaded[
                                dim_diff
                            ].iloc[0][0]
                            with xml.Member(Hierarchy="[{0}].[{0}]".format(dim_diff)):
                                xml.UName(
                                    "[{0}].[{0}].[{1}].[{2}]".format(
                                        dim_diff,
                                        self.executor.tables_loaded[dim_diff].columns[
                                            0
                                        ],
                                        column_attribut,
                                    )
                                )
                                xml.Caption(str(column_attribut))
                                xml.LName(
                                    "[{0}].[{0}].[{1}]".format(
                                        dim_diff,
                                        self.executor.tables_loaded[dim_diff].columns[
                                            0
                                        ],
                                    )
                                )
                                xml.LNum("0")
                                xml.DisplayInfo("2")

                        # Hierarchize
                        if len(self.executor.selected_measures) <= 1 and (
                            self.executor.parser.hierarchized_tuples()
                            or self.executor.facts in self.columns_desc["where"]
                        ):
                            with xml.Member(Hierarchy="[Measures]"):
                                xml.UName(
                                    "[Measures].[{}]".format(self.executor.measures[0])
                                )
                                xml.Caption("{}".format(self.executor.measures[0]))
                                xml.LName("[Measures]")
                                xml.LNum("0")
                                xml.DisplayInfo("0")

        return str(xml)

    def generate_response(self):
        """generate the xmla response.

        :return: xmla response as string
        """

        if self.mdx_query == "":
            # check if command contains a query
            xml = xmlwitch.Builder()
            with xml["return"]:
                xml.root(xmlns="urn:schemas-microsoft-com:xml-analysis:empty")

            return str(xml)

        else:

            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:mddataset",
                    **{
                        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                    }
                ):
                    xml.write(execute_xsd)
                    with xml.OlapInfo:
                        with xml.CubeInfo:
                            with xml.Cube:
                                xml.CubeName("Sales")
                                xml.LastDataUpdate(
                                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    xmlns="http://schemas.microsoft.com/analysisservices/2003/engine",
                                )
                                xml.LastSchemaUpdate(
                                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    xmlns="http://schemas.microsoft.com/analysisservices/2003/engine",
                                )
                        xml.write(self.generate_cell_info())
                        with xml.AxesInfo:
                            xml.write(self.generate_axes_info())
                            xml.write(self.generate_axes_info_slicer())

                    with xml.Axes:
                        xml.write(self.generate_xs0())
                        xml.write(self.generate_slicer_axis())

                    with xml.CellData:
                        xml.write(self.generate_cell_data())
            return str(xml)
