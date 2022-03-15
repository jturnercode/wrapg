import psycopg
from psycopg import sql
import pandas as pd
from numpy import nan
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
# TODO: Conditionally import config else initialize as {}
# TODO: Implement copy function
# ===========================================================================

conn_import: dict = {
    "user": config.DB_USERNAME,
    "password": config.DB_PASSWORD,
    "host": config.DB_HOST,
    "dbname": config.DB_NAME,
    "port": config.DB_PORT,
}


def _data_transform(data_structure):
    """Internal function checks passed data structure and
    returns list of columns and list of tuples(rows)

    Args:
        data_structure (Any): data needing to be inserted/
        updated in postgres (type: dataframe,
        list/tuple of dict, dict)

    Returns:
        column, row data: list of columns, list of tuples(rows)
    """
    # =================== TODO ===================
    # TODO: handle json data structure
    # TODO: handle iterator?

    # pattern matching data structure passed
    match data_structure:
        case pd.DataFrame():
            # print("type -> dataframe")
            columns = data_structure.columns
            # Need to replace all NaN to None, pg sees nan as str
            df = data_structure.replace(nan, None)
            rows = list(df.itertuples(index=False, name=None))
            return columns, rows

        # list/tuple of dictionaries
        # Checks if all object in list/tuple are dict
        # TODO: This may be place for improvement if large datasets?
        case list(d) | tuple(d) if all(isinstance(x, dict) for x in d):
            # print("type -> list of dictionaries")
            # Pass data into dataframe to make sure it is in
            # consistent order and account for non-uniform data
            # This allows for one dynamic query vs having to
            # recreate query for each row(dictionary)
            df = pd.DataFrame(data_structure)
            columns = df.columns
            # TODO: below will convert intergers to float if column has nan, fix!
            df = df.replace(nan, None)
            rows = list(df.itertuples(index=False, name=None))
            return columns, rows

        case dict():
            # print("type -> dictionary")
            columns = data_structure.keys()
            rows = [tuple(data_structure.values())]
            return columns, rows

        case []:
            raise ValueError(
                f"Empty list passed, must contain at least one dictionary."
            )

        case _:
            raise ValueError(f"Unsupported data structure passed.")


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


def insert(data: list[dict] | pd.DataFrame, table: str, conn_kwargs: dict = None):

    """Function for SQL's INSERT
    Add a row(s) into specified table

    Args:
        data (list[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    columns, rows = _data_transform(data)

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
            # cur.execute("INSERT INTO table (col1, col2) VALUES (%s, %s)",
            #  (300, "vehicles"))

            # dynamic insert query for dictionaries
            # qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            #     sql.Identifier(table),
            #     sql.SQL(", ").join(map(sql.Identifier, columns)),
            #     sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # )

            # Dynamic insert query for list of tuples
            qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
            )
            # print(qry.as_string(conn))

            # Simple insert with on variable {}
            cur.executemany(query=qry, params_seq=rows)

            # Make the changes to the database persistent
            conn.commit()


def insert_ignore(
    data: list[dict] | pd.DataFrame, table: str, keys: list, conn_kwargs: dict = None
):
    """Function for SQL's INSERT ON CONFLICT DO NOTHING
    Add a row into specified table if the row with specified keys does already exist.
    If rows with matching keys exist no change is made.
    Automatically creates unique constriant if one does not exist for keys provided.

    Args:
        data (list[dict] | pd.DataFrame): data in form of dict, list of dict, or dataframe
        table (str): name of database table
        keys (list): list of columns
        conn_kwargs (dict, optional): Specify/overide conn kwargs. See full list of options,
        https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS.
        Defaults to None, recommend importing via .env file.
    """

    # Inspect data and return columns and rows
    columns, rows = _data_transform(data)

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
            qry = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
            ).format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns)),
                sql.SQL(", ").join(map(sql.Identifier, keys)),
            )
            print(qry.as_string(conn))

            # cur.executemany(query=qry, params_seq=rows)
            try:
                cur.executemany(query=qry, params_seq=rows)

            # Catch no unique constriant error
            except errors.InvalidColumnReference as e:
                print(">>> Error: ", e)
                print("> Rolling back, attempt creation of new constriant...")
                conn.rollback()

                # Alter table and create new unique constriant & try again
                try:
                    constraint_name = f'{table}_{"_".join(keys)}_idx'
                    alter = sql.SQL(
                        "ALTER TABLE {} ADD CONSTRAINT {} UNIQUE ({});"
                    ).format(
                        sql.Identifier(table),
                        sql.Identifier(constraint_name),
                        sql.SQL(", ").join(map(sql.Identifier, keys)),
                    )
                    # print(alter.as_string(conn))

                    cur.execute(query=alter)

                    # Now execute previous insert_ignore statement
                    cur.executemany(query=qry, params_seq=rows)

                except Exception as alter_error:
                    print(">>> Error: ", alter_error)
                    quit()

            # Handle all other exceptions
            except Exception as ee:
                print("Exception: ", ee.__init__)
                quit()

            # Make the changes to the database persistent
            conn.commit()
