def create_insert(con):

    statement = """
    CREATE TABLE IF NOT EXISTS facts (
        day text,
        city text,
        licence text,
        amount integer,
        count integer);
    """
    con.execute(statement)

    statement = """
    INSERT INTO facts (day, city, licence, amount, count) VALUES
    ('May 12,2010','Madrid','Personal',1,84),
    ('May 13,2010','Barcelona','Personal',2,841),
    ('May 14,2010','Paris','Personal',4,2),
    ('May 15,2010','Lausanne','Personal',8,231),
    ('May 16,2010','Lausanne','Corporate',16,4),
    ('May 17,2010','Lausanne','Partnership',32,65),
    ('May 18,2010','Zurich','Partnership',64,64),
    ('May 19,2010','Geneva','Corporate',128,13),
    ('May 20,2010','New York','Corporate',256,12),
    ('May 21,2010','New York','Corporate',512,564);
    """

    con.execute(statement)

    statement = """
    CREATE TABLE IF NOT EXISTS geography (
        continent text,
        country text,
        city text);
    """
    con.execute(statement)

    statement = """
    INSERT INTO geography (continent, country, city) VALUES
    ('America','Canada','Quebec'),
    ('America','Canada','Toronto'),
    ('America','United States','Los Angeles'),
    ('America','United States','New York'),
    ('America','United States','San Francisco'),
    ('America','Mexico','Mexico'),
    ('America','Venezuela','Caracas'),
    ('Europe','France','Paris'),
    ('Europe','Spain','Barcelona'),
    ('Europe','Spain','Madrid'),
    ('Europe','Spain','Valencia'),
    ('Europe','Switzerland','Geneva'),
    ('Europe','Switzerland','Lausanne'),
    ('Europe','Switzerland','Zurich');
    """
    con.execute(statement)

    statement = """
    CREATE TABLE IF NOT EXISTS product (
        company text,
        article text,
        licence text);
    """
    con.execute(statement)

    statement = """
        INSERT INTO product (company, article, licence) VALUES
        ('Crazy Development','olapy','Corporate'),
        ('Crazy Development','olapy','Partnership'),
        ('Crazy Development','olapy','Personal'),
        ('Crazy Development','olapy','Startup');
    """
    con.execute(statement)

    statement = """
    CREATE TABLE IF NOT EXISTS time (
        year integer,
        quarter text,
        month text,
        day text
    );
    """
    con.execute(statement)

    statement = """INSERT INTO time (year, quarter, month, day) VALUES
              (2010, 'Q2 2010', 'May 2010', 'May 12,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 13,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 14,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 15,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 16,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 17,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 18,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 19,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 20,2010'),
              (2010, 'Q2 2010', 'May 2010', 'May 21,2010');"""

    con.execute(statement)


def drop_tables(con):

    statement = """
    DROP TABLE facts;
    """
    con.execute(statement)

    statement = """
    DROP TABLE geography;
    """
    con.execute(statement)

    statement = """
    DROP TABLE product;
    """
    con.execute(statement)

    statement = """
    DROP TABLE time;
    """
    con.execute(statement)
