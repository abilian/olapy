from __future__ import absolute_import, division, print_function, \
    unicode_literals


def create_insert(con, custom=False):
    if custom:
        custom_create_insert(con)
    else:
        normal_create_insert(con)


def custom_create_insert(con):
    statement = """
    CREATE TABLE IF NOT EXISTS food_facts (
        product_id integer,
        warehouse_id integer,
        store_id integer,
        units_ordered integer,
        units_shipped integer,
        warehouse_sales numeric,
        warehouse_cost numeric,
        supply_time integer,
        store_invoice numeric
    );
    """
    con.execute(statement)

    statement = """
    INSERT INTO
        food_facts (product_id, warehouse_id, store_id, units_ordered, units_shipped,
        warehouse_sales, warehouse_cost, supply_time, store_invoice)
    VALUES
        (1,1,4,53,7,7.4774,2.6171,4,3.0358),
        (2,1,1,84,62,50.5548,28.8162,1,32.8505),
        (3,2,5,24,24,29.8224,15.5076,2,17.2134),
        (1,1,1,65,65,46.592,27.0234,3,30.2662),
        (4,4,2,67,67,57.7406,33.4895,4,38.5129),
        (1,3,1,82,45,80.433,38.6078,4,43.2407),
        (2,1,1,45,45,69.696,24.3936,2,26.833),
        (5,2,2,26,26,38.532,15.0275,1,17.1313),
        (5,1,1,43,43,98.6893,54.2791,3,62.421);
    """

    con.execute(statement)

    statement = """
        CREATE TABLE IF NOT EXISTS product (
            id integer,
            brand_name text,
            product_name text,
            sku integer,
            srp numeric,
            gross_weight numeric,
            net_weight numeric,
            recyclable_package integer,
            low_fat integer,
            units_per_case integer,
            cases_per_pallet integer,
            shelf_width numeric,
            shelf_height numeric,
            shelf_depth numeric);
        """
    con.execute(statement)

    statement = """
    INSERT INTO product (id, brand_name, product_name, sku, srp, gross_weight, net_weight,
        recyclable_package,low_fat, units_per_case, cases_per_pallet, shelf_width, shelf_height, shelf_depth)
    VALUES
        (1, 'Washington', 'Washington Berry Juice', 554270458, 2.85, 8.39, 6.39, 0, 0, 30, 14, 16.9, 12.6, 7.4),
        (2, 'Washington', 'Washington Mango Drink', 2027221987, 0.74, 7.42, 4.42, 0, 1, 18, 8, 13.4, 3.71, 22.6),
        (3, 'Washington', 'Washington Strawberry Drink', -1701770219, 0.83, 13.1, 11.1, 1, 1, 17, 13, 14.4, 11, 7.77),
        (4, 'Washington', 'Washington Cream Soda', -12353693, 3.64, 10.6, 9.6, 1, 0, 26, 10, 22.9, 18.9, 7.93),
        (5, 'Washington', 'Washington Diet Soda', -338154481, 2.19, 6.66, 4.65, 1, 0, 7, 10, 20.7, 21.9, 19.2);
    """
    con.execute(statement)

    statement = """
        CREATE TABLE IF NOT EXISTS store (
            id integer,
            store_type text,
            store_name text,
            store_number integer,
            store_street_address text,
            store_city text,
            store_state text,
            store_postal_code integer,
            store_country text,
            store_manager text,
            store_phone text,
            store_fax text,
            first_opened_date text,
            last_remodel_date text,
            store_sqft integer,
            grocery_sqft integer,
            frozen_sqft integer,
            meat_sqft integer,
            coffee_bar integer,
            video_store integer,
            salad_bar integer,
            prepared_food integer,
            florist integer
            );
        """
    con.execute(statement)

    statement = """
    INSERT INTO store (id, store_type, store_name, store_number, store_street_address, store_city,
    store_state,store_postal_code, store_country, store_manager, store_phone, store_fax, first_opened_date,
    last_remodel_date,store_sqft, grocery_sqft, frozen_sqft, meat_sqft, coffee_bar, video_store,
    salad_bar,prepared_food, florist)
    VALUES
    (0, 'HeadQuarters', 'HQ', 0, '1 Alameda Way', 'Alameda', 'CA', 55555, 'USA',
     '', '', '', '', '', 0, 0, 0, 0, 0, 0, 0, 0, 0),
    (1, 'Supermarket', 'Store 1', 1, '2853 Bailey Rd', 'Acapulco', 'Guerrero',
     55555, 'Mexico', 'Jones', '262-555-5124',
     '262-555-5121', '1982-01-09 00:00:00', '1990-12-05 00:00:00', 23593, 17475,
     3671, 2447, 0, 0, 0, 0, 0),
    (2, 'Small Grocery', 'Store 2', 2, '5203 Catanzaro Way', 'Bellingham', 'WA',
     55555, 'USA', 'Smith', '605-555-8203',
     '605-555-8201', '1970-04-02 00:00:00', '1973-06-04 00:00:00', 28206, 22271,
     3561, 2374, 1, 0, 0, 0, 0),
    (3, 'Supermarket', 'Store 3', 3, '1501 Ramsey Circle', 'Bremerton', 'WA',
     55555, 'USA', 'Davis', '509-555-1596',
     '509-555-1591', '1959-06-14 00:00:00', '1967-11-19 00:00:00', 39696, 24390,
     9184, 6122, 0, 0, 1, 1, 0),
    (4, 'Gourmet Supermarket', 'Store 4', 4, '433 St George Dr', 'Camacho',
     'Zacatecas', 55555, 'Mexico', 'Johnson',
     '304-555-1474', '304-555-1471', '1994-09-27 00:00:00',
     '1995-12-01 00:00:00', 23759, 16844, 4149, 2766, 1, 0, 1, 1, 1),
    (5, 'Small Grocery', 'Store 5', 5, '1250 Coggins Drive', 'Guadalajara',
     'Jalisco', 55555, 'Mexico', 'Green',
     '801-555-4324', '801-555-4321', '1978-09-18 00:00:00',
     '1991-06-29 00:00:00', 24597, 15012, 5751, 3834, 1, 0, 0, 0, 0);
    """
    con.execute(statement)

    statement = """
    CREATE TABLE IF NOT EXISTS warehouse (
        id integer,
        warehouse_name text,
        wa_address1 text,
        wa_address2 text,
        wa_address3 text,
        wa_address4 text,
        warehouse_city text,
        warehouse_state_province text,
        warehouse_postal_code integer,
        warehouse_country text,
        warehouse_owner_name text,
        warehouse_phone text,
        warehouse_fax text
    );
    """
    con.execute(statement)

    statement = """
    INSERT INTO
    warehouse (id,warehouse_name,wa_address1,wa_address2,
    wa_address3,wa_address4,warehouse_city,warehouse_state_province,
    warehouse_postal_code,warehouse_country,warehouse_owner_name,
    warehouse_phone,warehouse_fax)
    VALUES
    (1, 'Salka Warehousing', '9716 Alovera Road', '', '', '', 'Acapulco',
     'Guerrero', 55555, 'Mexico', '', '821-555-1658', '594-555-2908'),
    (2, 'Foster Products', '958 Hilltop Dr', '', '', '', 'Bellingham', 'WA',
     55555, 'USA', '', '315-555-8947', '119-555-3826'),
    (3, 'Destination, Inc.', '4162 Euclid Ave', '', '', '', 'Bremerton', 'WA',
     55555, 'USA', '', '517-555-3022', '136-555-4501'),
    (4, 'Anderson Warehousing', '5657 Georgia Dr', '', '', '', 'Camacho',
     'Zacatecas', 55555, 'Mexico', '', '681-555-1655', '946-555-4848'),
    (5, 'Focus, Inc.', '9116 Tice Valley Blv.', '', '', '', 'Guadalajara',
     'Jalisco', 55555, 'Mexico', '', '344-555-5530', '379-555-9065');
    """
    con.execute(statement)


def normal_create_insert(con):
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


def drop_tables(con, custom=False):
    if custom:
        custom_drop_tables(con)
    else:
        normal_drop_tables(con)


def custom_drop_tables(con):
    for table_name in ("food_facts", "store", "product", "warehouse"):
        statement = "DROP TABLE {};".format(table_name)
        con.execute(statement)


def normal_drop_tables(con):
    for table_name in ("facts", "geography", "product", "time"):
        statement = "DROP TABLE {};".format(table_name)
        con.execute(statement)
