from collections.abc import Iterable
import psycopg
from psycopg import sql, errors
import pandas as pd
from wrapg import util
import config


# ===========================================================================
#  ?                                wrapg
#  @author         :  jturnercode
#  @createdOn      :  2/24/2022
#  @description    :  Wrapper around pyscopg (version 3). Use to easily run sql
# functions inside python code to interact with postgres database.
# Inspired by dataset library that wraps sqlalchemy
# ================================= TODO ================================
# TODO: Can query() return explain analyse info?
# TODO: Conditionally import config/env else initialize as {}
# TODO: Implement copy function, update, delete
# ===========================================================================

conn_import: dict = {
    "user": config.DB_USERNAME,
    "password": config.DB_PASSWORD,
    "host": config.DB_HOST,
    "dbname": config.DB_NAME,
    "port": config.DB_PORT,
}


def query(raw_sql: str, to_df=False, conn_kwargs: dict = None):
    """Function to send raw sql query to postgres db.

    Args:
        raw_sql (str): sql query in string form.
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
        to_df (bool, optional): Return results of query in dataframe. Defaults to False.

    Returns:
        _type_: iterator or Dataframe
    """

    # Initialize conn_kwargs to empty dict if no arguments passed
    # Merge args into conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final connection args to pass to connect()
    # Set default return type (row factory) to dictionary, can be overwritten with kwargs
    conn_final = {"row_factory": psycopg.rows.dict_row, **conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Pass raw_sql to execute()
            # example: cur.execute("SELECT * FROM tablename WHERE id = 4")
            cur.execute(query=raw_sql)

            # Used for testing output of raw_sql
            # print("rowcount: ", cur.rowcount)
            # print("statusmessage: ", cur.statusmessage)
            # print(cur)

            # .statusmessage returns string of type of operation processed
            # If 'select' in status message return records as df or iter
            if "SELECT" in cur.statusmessage:
                if to_df is True:
                    return pd.DataFrame(cur, dtype="object")

                # Save memory return iterator
                return iter(cur.fetchall())


def insert(data: Iterable[dict] | pd.DataFrame, table: str, conn_kwargs: dict = None):

    """Function for SQL's INSERT

    Add a row(s) into specified table

    Args:
        data (Iterable[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    columns, rows, uniform = util.data_transform(data)

    # Initialize conn_kwargs to empty dict if no arguments passed
    # Merge args into conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final conn args to pass to connect()
    conn_final = {**conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Typ insert statement format
            # INSERT INTO table (col1, col2) VALUES (300, "vehicles");

            if uniform == 1:
                # Dynamic insert query for dictionaries
                qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(map(sql.Placeholder, columns)),
                    # Dynamic insert query for list of tuples
                    # sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                )
                # print(qry.as_string(conn))
                # One insert for all dictionaries
                cur.executemany(query=qry, params_seq=rows)

            else:
                # For non uniform data
                for row in rows:
                    # Dynamic insert query for dictionaries
                    qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, tuple(row))),
                        sql.SQL(", ").join(map(sql.Placeholder, tuple(row))),
                    )
                    # Seperate insert for each dictionary
                    cur.execute(query=qry, params=row)

            # Make the changes to the database persistent
            conn.commit()


def insert_ignore(
    data: Iterable[dict] | pd.DataFrame,
    table: str,
    keys: list,
    conn_kwargs: dict = None,
):
    """Function for SQL's INSERT ON CONFLICT DO NOTHING

    Add a row into specified table if the row with specified keys does not already exist.
    If rows with matching keys exist no change is made.
    Automatically creates unique index if one does not exist for keys provided.

    Args:
        data (Iterable[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        keys (list): Iterable of columns
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    # Inspect data and return columns and rows
    columns, rows, uniform = util.data_transform(data)

    # Initialize conn_kwargs to empty dict if no arguments passed
    # Merge args into conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final conn parameters to pass to connect()
    # Set return default type to dictionary, can be overwritten with kwargs
    conn_final = {**conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # INSERT INTO table (name, email)
            # VALUES('Dave','dave@yahoo.com')
            # ON CONFLICT (name)
            # DO NOTHING;

            # =================== Ignore_Insert Qry ==================
            try:
                if uniform == 1:
                    qry = sql.SQL(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                    ).format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, columns)),
                        sql.SQL(", ").join(map(sql.Placeholder, columns)),
                        # this is technically known as 'conflict target'
                        sql.SQL(", ").join(map(sql.Identifier, keys)),
                    )
                    # print(qry.as_string(conn))
                    cur.executemany(query=qry, params_seq=rows)

                else:
                    for row in rows:
                        qry = sql.SQL(
                            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                        ).format(
                            sql.Identifier(table),
                            sql.SQL(", ").join(map(sql.Identifier, tuple(row))),
                            sql.SQL(", ").join(map(sql.Placeholder, tuple(row))),
                            # this is technically known as 'conflict target'
                            sql.SQL(", ").join(map(sql.Identifier, keys)),
                        )
                    # print(qry.as_string(conn))
                    cur.execute(query=qry, params=row)

            # Catch no unique constriant error
            except errors.InvalidColumnReference as e:
                print(">>> Error: ", e)
                print("> Rolling back, attempt creation of new constriant...")
                conn.rollback()

                # Create new unique index & try again
                try:
                    uix_name = f'{table}_{"_".join(keys)}_uix'
                    uix_sql = sql.SQL("CREATE UNIQUE INDEX {} ON {} ({});").format(
                        sql.Identifier(uix_name),
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, keys)),
                    )
                    # print(idx_sql.as_string(conn))
                    cur.execute(query=uix_sql)

                    # Try again to execute qry
                    if uniform == 1:
                        # Now execute previous insert_ignore statement
                        cur.executemany(query=qry, params_seq=rows)

                    else:
                        for row in rows:
                            qry = sql.SQL(
                                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                            ).format(
                                sql.Identifier(table),
                                sql.SQL(", ").join(map(sql.Identifier, tuple(row))),
                                sql.SQL(", ").join(map(sql.Placeholder, tuple(row))),
                                # this is technically known as 'conflict target'
                                sql.SQL(", ").join(map(sql.Identifier, keys)),
                            )
                        # print(qry.as_string(conn))
                        cur.execute(query=qry, params=row)

                except Exception as indx_error:
                    print(">>> Error: ", indx_error)
                    quit()

            # Handle all other exceptions, not InvalidColumn Reference
            except Exception as ee:
                print("Exception: ", ee.__init__)
                quit()

            # Make the changes to the database persistent
            conn.commit()


def upsert(
    data: Iterable[dict] | pd.DataFrame,
    table: str,
    keys: list,
    conn_kwargs: dict = None,
):
    """Function for SQL's INSERT ON CONFLICT DO UPDATE SET

    Add a row into specified table if the row with specified keys does not already exist.
    If rows with matching keys exist, update row values.
    Automatically creates unique index if one does not exist for keys provided.

    Args:
        data (list[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        keys (list): list of columns
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    # Inspect data and return columns and rows
    columns, rows, uniform = util.data_transform(data)

    # Initialize conn_kwargs to empty dict if no arguments passed
    # Merge args into conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final conn parameters to pass to connect()
    # Set return default type to dictionary, can be overwritten with kwargs
    conn_final = {**conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Typical syntax
            # INSERT INTO table (name, email)
            # VALUES('Dave','dave@yahoo.com')
            # ON CONFLICT (name)
            # DO UPDATE SET email=excluded.email
            # WHERE ...;

            # Function to compose col=excluded.col sql for update
            def set_str(cols: list):

                col_update = []
                for col in cols:
                    col_update.append(
                        sql.SQL("{}=excluded.{}").format(
                            sql.Identifier(col),
                            sql.Identifier(col),
                        )
                    )
                return col_update

            # =================== Upsert Qry ==================
            # TODO: add WHERE clause to upsert qry; Do i need it?
            """
            WHERE excluded.validDate>phonebook2.validDate;
            sudo code fro add where
            def sql_to_format()
                if where:
                    return sql.SQL(statement with where)
                else:
                    return sql.SQL(statement without where)
            """
            qry = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                # this is technically known as 'conflict target'
                sql.SQL(", ").join(map(sql.Identifier, keys)),
                sql.SQL(", ").join(set_str(columns)),
            )
            # print(qry.as_string(conn))

            try:
                cur.executemany(query=qry, params_seq=rows)

            # Catch no unique index error
            except errors.InvalidColumnReference as e:
                print(">>> Error: ", e)
                print("> Rolling back, attempt creation of new constriant...")
                conn.rollback()

                # Add unique index & try again
                try:
                    uix_name = f'{table}_{"_".join(keys)}_uix'
                    uix_sql = sql.SQL("CREATE UNIQUE INDEX {} ON {} ({});").format(
                        sql.Identifier(uix_name),
                        sql.Identifier(table),
                        sql.SQL(", ").join(map(sql.Identifier, keys)),
                    )
                    # print(idx_sql.as_string(conn))

                    # Unique constriants caused problems with overlapping keys?
                    # constraint_name = f'{table}_{"_".join(keys)}_cst'
                    # alter = sql.SQL(
                    #     "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});"
                    # ).format(
                    #     sql.Identifier(table),
                    #     sql.Identifier(constraint_name),
                    #     sql.SQL(", ").join(map(sql.Identifier, keys)),
                    # )

                    cur.execute(query=uix_sql)

                    # Now execute previous upsert statement
                    cur.executemany(query=qry, params_seq=rows)

                except Exception as indx_error:
                    print(">>> Error: ", indx_error)
                    quit()

            # Handle all other exceptions
            except Exception as ee:
                print("Exception: ", ee.__init__)
                quit()

            # Make the changes to the database persistent
            conn.commit()


"""
# TODO: **upsert without creating auto-index or constriants
**Add as option to exisiting upsert(auto_uix=False)
def upsert_wo_idx():
    Sudo code
    This function would be run inside a for loop within a transaction
    every iteration would result in a update or an insert

    try:
        update() on keys
        if update() return value is 0
        then insert()
    this code should be much slower but not require creating
    unique constriants which may be hard to manage especially
    for novice users
"""

# ================================= UPDATE ================================
def update(
    data: list[dict] | pd.DataFrame,
    table: str,
    keys: Iterable,
    conn_kwargs: dict = None,
):
    """Function for SQL's UPDATE

    If rows with matching keys exist, update row values.
    The columns that do not appear in the 'data' retain their original values.
    Make sure data has keys specified in function.

    Args:
        data (list[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        keys (Iterable): Iterable of column names synonymous with sql WHERE
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    # Inspect data and return columns and rows
    columns, rows, uniform = util.data_transform(data)

    # Initialize conn_kwargs to empty dict if no arguments passed
    # Merge args into conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final conn parameters to pass to connect()
    # Set return default type to dictionary, can be overwritten with kwargs
    conn_final = {**conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Syntax
            # UPDATE table_name
            # SET column1 = value1,
            #     column2 = value2,
            #     ...
            # TODO: Where clause is optional, add code if None update all
            # WHERE column = value, ...
            # RETURNING * | output_expression AS output_name;

            # Function to compose col=value sql str
            def column_value_str(column_names: Iterable):
                """Create psycopg composable sql string for
                variable number of columns/value pairs
                ie column=value scenerios, col=%(col)s

                Args:
                    column_names (Iterable): column names
                """
                # function used to map to column names
                def set_sql(col):
                    return sql.SQL("{}={}").format(
                        sql.Identifier(col),
                        sql.Placeholder(col),
                    )

                return map(set_sql, column_names)

            # =================== Update Qry ==================
            if uniform == 1:
                # print("> Uniform Data..")
                qry = sql.SQL("UPDATE {} SET {} WHERE {}").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(column_value_str(columns)),
                    sql.SQL(", ").join(column_value_str(keys)),
                )
                print(qry.as_string(conn))

                cur.executemany(query=qry, params_seq=rows)
            else:
                # print(">> Non-Uniform Data...")
                for row in rows:
                    qry = sql.SQL("UPDATE {} SET {} WHERE {}").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(column_value_str(tuple(row))),
                        sql.SQL(", ").join(column_value_str(keys)),
                    )

                    cur.execute(query=qry, params=row)

            # Make the changes to the database persistent
            conn.commit()
