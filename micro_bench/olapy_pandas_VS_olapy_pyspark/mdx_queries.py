query1 = """SELECT
           FROM [foodmart]
           WHERE ([Measures].[supply_time])
           CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

query2 = """
SELECT NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{{
          [Product].[Product].[All brand_name].Members}}}, {
          [Product].[Product].[brand_name].[Pearl]})))
          DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
          FROM [foodmart]
          WHERE ([Measures].[supply_time])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""

query3 = """
SELECT NON EMPTY CrossJoin(Hierarchize(AddCalculatedMembers({
          [Product].[Product].[All brand_name].Members})), Hierarchize(AddCalculatedMembers({
          [Store].[Store].[All store_type].Members})))
          DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
          FROM [foodmart]
          WHERE ([Measures].[supply_time])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
"""
