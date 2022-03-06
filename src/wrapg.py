import psycopg
from psycopg import sql
import pandas as pd
from numpy import nan
import config


# ===========================================================================
#  ?                                wrapg
#  @author         :  jturnercode
#  @createdOn      :  2/24/2022
#  @description    :  Wrapper around pyscopg (version 3). Use to easily run functions
# inside python code to interact with postgres database. Inspired by dataset library
# that wraps sqlalchemy
# ================================= TODO ================================
# TODO: Can query() return explain analyse info?
# TODO: Conditionally import config else initialize as {}
# TODO: Conditionally import pandas?
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
    returns list of columns and list of tuples(for rows)

    Args:
        data_structure (Any): data needing to be inserted,
        updated in postgres (typ: dataframe, list of dict)

    Returns:
        column, row data: list of columns, list of tuples(rows)
    """

    # TODO: handle json data structure
    # TODO: handle iterator?
    match data_structure:

        # case isinstance(data_structure, pd.DataFrame):
        case pd.DataFrame():
            # print("type -> Dataframe")
            columns = data_structure.columns
            # Need to replace all NaN to None
            df = data_structure.replace(nan, None)
            rows = list(df.itertuples(index=False, name=None))
            return columns, rows

        # list/tuple of dictionaries
        case list(d) | tuple(d) if all(isinstance(x, dict) for x in d):
            # print(all(isinstance(x, dict) for x in d))
            # print("type -> list of dictionaries")
            df = pd.DataFrame(data_structure)
            columns = df.columns
            # TODO: below may convert intergers to float if column has nan
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


def query(raw_sql: str, conn_kwargs: dict = None, to_df=False):

    """
    Send raw sql query to postgres db.
    See list of conn_kwargs here:
    https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

    Returns:
        _type_: _description_
    """

    # initialize conn_kwargs to empty dict if no arguments passed
    # to merge in conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final connection parameters to pass to connect()
    # Set default return type (row factory) to dictionary, can be overwritten with kwargs
    conn_final = {"row_factory": psycopg.rows.dict_row, **conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Pass raw_sql to execute()
            # example: cur.execute("SELECT * FROM test3 WHERE id = 2")
            cur.execute(query=raw_sql)

            # Used for testing output of raw_sql
            # print("rowcount: ", cur.rowcount)
            # print("statusmessage: ", cur.statusmessage)

            # .statusmessage returns string of type of operation processed
            # If 'select' in status message return records as df or iter
            if "SELECT" in cur.statusmessage:
                if to_df is True:
                    return pd.DataFrame(cur, dtype="object")

                # Save memory return iterator
                return iter(cur.fetchall())


def insert(data: list[dict] | pd.DataFrame, table: str, conn_kwargs: dict = None):

    columns, rows = _data_transform(data)

    # initialize conn_kwargs to empty dict if no arguments passed
    # to merge in conn_final
    if conn_kwargs is None:
        conn_kwargs = {}

    # Final conn parameters to pass to connect()
    # Set return default type to dictionary, can be overwritten with kwargs
    conn_final = {**conn_import, **conn_kwargs}

    # Connect to an existing database
    with psycopg.connect(**conn_final) as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # Typ insert statement format
            # cur.execute("INSERT INTO test3 (num, data) VALUES (%s, %s)",
            #  (300, "vehicles"))

            # Build dynamic query for dictionaries
            # qry = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            #     sql.Identifier(table),
            #     sql.SQL(", ").join(map(sql.Identifier, columns)),
            #     sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # )

            # Dynamic query for list of tuples
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
