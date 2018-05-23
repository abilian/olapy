from __future__ import absolute_import, division, print_function, \
    unicode_literals

query1 = """
    SELECT
    Hierarchize({[Measures].[Amount]}) ON COLUMNS
    FROM [sales]
"""

query6 = """
    SELECT NON EMPTY
    Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
    [Time].[Time].[Year].Members}}, {
    [Time].[Time].[Year].[2010]})}}, {
    [Time].[Time].[Quarter].[2010].[Q2 2010]})}}, {
    [Time].[Time].[Month].[2010].[Q2 2010].[May 2010]}))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [sales]
    WHERE ([Measures].[Amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query7 = """
    SELECT {(
    [Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 18,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 16,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 14,2010],
    [Geography].[Geography].[Continent].[Europe],[Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 12,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 13,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),
    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 15,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 17,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Product].[Product].[Company].[Crazy Development],
    [Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 19,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]

    )} ON 0
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS

    """

query9 = """
    SELECT
    {([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 19,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 17,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 15,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 13,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 12,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 14,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 16,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount]),

    ([Time].[Time].[Day].[2010].[Q2 2010].[May 2010].[May 18,2010],
    [Geography].[Geography].[Continent].[Europe],
    [Measures].[Amount])}

    ON 0
    FROM [sales]

    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""
