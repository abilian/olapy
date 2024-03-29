CUBE = "sales"

query1 = """
    SELECT
    Hierarchize({[Measures].[amount]}) ON COLUMNS
    FROM [sales]
"""

query2 = """
    SELECT
    Hierarchize({[geography].[economy].[partnership]}) ON COLUMNS
    FROM [sales]
"""

query3 = """
    SELECT
    Hierarchize(non empty {[geography].[geo].[country].Members}) ON COLUMNS,
    Hierarchize({[Measures].[amount]}) ON ROWS
    FROM [sales]
"""

query4 = """
    SELECT
    Hierarchize({[geography].[economy].[partnership]}) ON COLUMNS,
    Hierarchize(non empty {[geography].[geo].[country].Members}) ON ROWS
    FROM [sales]
"""

query5 = """
SELECT
    Hierarchize({[geography].[economy].[partnership].[EU],
    [geography].[economy].[partnership].[None],
    [geography].[economy].[partnership].[NAFTA]}) ON COLUMNS,
    {[product].[prod].[company].[Crazy Development],
    [product].[prod].[company].[company_test],
    [product].[prod].[company].[test_Development]} ON ROWS
    FROM [sales]
"""

# GENERATED BY EXCEL
query6 = """
    SELECT NON EMPTY
    Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
    [time].[time].[year].Members}}, {
    [time].[time].[year].[2010]})}}, {
    [time].[time].[quarter].[2010].[Q2 2010]})}}, {
    [time].[time].[month].[2010].[Q2 2010].[May 2010]})))
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME, HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [sales]
    WHERE ([Measures].[amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query7 = """
    SELECT {
        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 18,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 16,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 14,2010],
        [geography].[geography].[continent].[Europe],[Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 12,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 13,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),
        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 15,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 17,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([product].[product].[company].[Crazy Development],
        [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 19,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]
    )}
    ON 0
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """

query8 = """
    SELECT {
        ([geography].[geography].[country].[Europe].[Spain],
        [Measures].[amount]),

        ([geography].[geography].[country].[Europe].[France],
        [Measures].[amount]),

        ([geography].[geography].[country].[Europe].[Switzerland],
        [Measures].[amount])
    }

    ON 0
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query9 = """
    SELECT {
        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 19,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 17,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 15,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 13,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 12,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 14,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 16,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount]),

        ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 18,2010],
        [geography].[geography].[continent].[Europe],
        [Measures].[amount])
    }
    ON 0
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query11 = """
    SELECT FROM [sales]
    WHERE ([Measures].[amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query12 = """
    SELECT {[Measures].[amount],[Measures].[count]}
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query14 = """
    SELECT {[Measures].[count],[Measures].[amount]}
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS ,
    NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{[geography].[geography].[continent].Members}},
    {[geography].[geography].[continent].[Europe]})))
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON ROWS
    FROM [sales] CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS

"""

query15 = """
    SELECT {
        ([product].[product].[Crazy Development].[olapy].[Personal],
        [geography].[geography].[Europe].[Switzerland],[Measures].[amount]),
        ([product].[product].[Crazy Development].[olapy].[Corporate],
        [geography].[geography].[Europe].[Switzerland],[Measures].[amount]),
        ([product].[product].[Crazy Development].[olapy].[Personal],
        [geography].[geography].[Europe].[Spain],[Measures].[amount]),
        ([product].[product].[Crazy Development].[olapy].[Personal],
        [geography].[geography].[Europe].[France],[Measures].[amount]),
        ([product].[product].[Crazy Development].[olapy].[Partnership],
        [geography].[geography].[Europe].[Switzerland],[Measures].[amount])
    }
    ON 0
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query16 = """
    SELECT {
        [product].[product].[Crazy Development].[olapy].[Partnership],
        [product].[product].[Crazy Development].[olapy].[Personal],
        [product].[product].[Crazy Development].[olapy].[Corporate],
        [product].[product].[Crazy Development]}*{
        [geography].[geography].[America].[United States].[New York],
        [geography].[geography].[America].[United States],
        [geography].[geography].[America],
        [geography].[geography].[Europe].[Switzerland].[Geneva],
        [geography].[geography].[Europe].[Switzerland].[Lausanne],
        [geography].[geography].[Europe].[Switzerland],
        [geography].[geography].[Europe].[France].[Paris],
        [geography].[geography].[Europe].[France],
        [geography].[geography].[Europe],
        [geography].[geography].[America]
    } ON 0 FROM [sales]
"""

where = "WHERE [time].[calendar].[day].[May 12,2010]"

query_posgres1 = """
    SELECT
    Hierarchize(non empty {[geography].[geography].[country].Members}) ON COLUMNS,
    Hierarchize({[Measures].[amount]}) ON ROWS
    FROM [sales_postgres]
"""
query_posgres2 = """
    SELECT NON EMPTY
    Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
    [time].[time].[year].Members}}, {
    [time].[time].[year].[2010]})}}, {
    [time].[time].[quarter].[2010].[Q2 2010]})}}, {
    [time].[time].[month].[2010].[Q2 2010].[May 2010]}))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [sales_postgres]
    WHERE ([Measures].[amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query_postgres3 = """
    SELECT {(
    [time].[time].[day].[2010].[Q2 2010].[May 2010].[May 19,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 17,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 15,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 13,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 12,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 14,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 16,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count]),

    ([time].[time].[day].[2010].[Q2 2010].[May 2010].[May 18,2010],
    [geography].[geography].[continent].[Europe],
    [Measures].[count])}

    ON 0
    FROM [sales_postgres]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

custom_query1 = """
    SELECT
    FROM [main]
    WHERE ([Measures].[supply_time])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""
