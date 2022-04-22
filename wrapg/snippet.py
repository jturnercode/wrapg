import re
from typing import Iterable
from psycopg import sql, connect


# Regex to seperate sql_func from column name
__compiled_pattern = re.compile(pattern=r"\((\w*)\)")


# =================== Snippet Util Functions ===================


def check_for_func(sequence: Iterable) -> bool:
    """Used to determine if a list or tuple of
    columns passed into sql function has
    parethesis '()' which indicate a function
    that needs to be parsed out

    Args:
        sequence (Iterable): list/tupe of column names

    Returns:
        bool: True if function found
    """
    # make all elements strings
    it = map(str, sequence)
    combined = "".join(it)
    return "(" in combined


def extract_sqlfunc_colname(column_name: str):
    """If sql function passed with column name,
    seperate function from column name.
    ie Date(ts) -> (Date(, ts)

    Args:
        column_name (str): _description_

    Returns:
        str or tuple: column_name or tuple of func, column_name
    """

    # pattern = r"\((\w*)\)"
    result = re.search(__compiled_pattern, column_name)

    # Extract matching values of all groups
    if result:
        column = result.group(1)
        func = column_name.replace(column + ")", "").upper()
        return func, column

    # if no '(' return original column name
    return column_name


def colname_snip(column_detail: str | tuple):
    """Return escaped sql snippet, accomodate column names
    wrapped by sql functions.

    Args:
        column_detail (str | tuple): column_name as str or tuple (sql_func, column_name)

    Returns:
        Composed: snippet of sql statment
    """
    # return escaped column name
    if isinstance(column_detail, str):
        return sql.SQL("{}").format(
            sql.Identifier(column_detail),
        )

    # else if tuple return snippet if sql func wrapping column
    return sql.SQL("{}{})").format(
        sql.SQL(column_detail[0]),
        sql.Identifier(column_detail[1]),
    )


# =================== Unique Index Snippet ===================


def create_unique_index(table, keys):

    # Note name will include parenthsis if passed
    # unique index name
    uix_name = f'{table}_{"_".join(keys)}_uix'

    # if sql function in the any key
    if check_for_func(keys):

        # Return tuple of functions and key else just key
        seperated_keys = map(extract_sqlfunc_colname, keys)

        # sql snippet to create unique index
        return sql.SQL("CREATE UNIQUE INDEX {} ON {} ({});").format(
            sql.Identifier(uix_name),
            sql.Identifier(table),
            sql.SQL(", ").join(map(colname_snip, seperated_keys)),
        )

    # sql snippet to create unique index
    return sql.SQL("CREATE UNIQUE INDEX {} ON {} ({});").format(
        sql.Identifier(uix_name),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, keys)),
    )


# =================== Upsert Snippets ===================

# Function to compose col=excluded.col sql for update
def exclude_sql(col):
    return sql.SQL("{}=EXCLUDED.{}").format(
        sql.Identifier(col),
        sql.Identifier(col),
    )


def upsert_snip(table: str, columns, keys):

    # if sql function in the any key
    if check_for_func(keys):

        # Return tuple of (function, key) else just key
        func_keys = map(extract_sqlfunc_colname, keys)

        return sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # conflict target
            sql.SQL(", ").join(map(colname_snip, func_keys)),
            # set new values
            sql.SQL(", ").join(map(exclude_sql, columns)),
        )

    return sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(map(sql.Placeholder, columns)),
        # conflict target
        sql.SQL(", ").join(map(sql.Identifier, keys)),
        # set new values
        sql.SQL(", ").join(map(exclude_sql, columns)),
    )


# =================== Insert_ignore Snippet ===================


def insert_ignore_snip(table: str, columns, keys):

    # if sql function in the any key
    if check_for_func(keys):

        # Return tuple of functions and key else just key
        seperated_keys = map(extract_sqlfunc_colname, keys)

        return sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # conflict target
            sql.SQL(", ").join(map(colname_snip, seperated_keys)),
        )

    return sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
    ).format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(map(sql.Placeholder, columns)),
        # conflict target
        sql.SQL(", ").join(map(sql.Identifier, keys)),
    )


# =================== Delete Snippet ===================

# Note: Seperated from where_snip() to possibly reuse
# on other dictionaries needing to be composed for other
# snippets in future 
def compose_key_value(key_value: tuple) -> tuple:
    """Take key_value tuple from dictionary via .items()
    and create a composed key_value tuple for use in creating
    snippets.

    Args:
        key_value (tuple): key value pair from dictionary

    Returns:
        tuple: composed key value
    """

    colname, value = key_value

    # TODO: ? add >, <, <>, etc to value to process data other than '='
    # composed value to literal type
    composed_value = sql.Literal(value)

    # Check if colname has a function
    if "(" in colname:
        # pattern = r"\((\w*)\)"
        result = re.search(__compiled_pattern, colname)

        # Extract matching values of all groups
        # this is done to escape column name
        column = result.group(1)
        func = colname.replace(column + ")", "").upper()

        composed_column = sql.SQL("{}{})").format(sql.SQL(func), sql.Identifier(column))

        return composed_column, composed_value

    # composed column if no func found
    composed_column = sql.Identifier(colname)
    return composed_column, composed_value




# Note: Seperated from compose_key_value() as there
# may be need for compose other dictionaries in future 
def where_snip(colname_value: tuple):
    """Represent where clause colname=value

    Args:
        colname_value (tuple): _description_

    Returns:
        _type_: _description_
    """

    colname, value = colname_value

    # return where clause snip for final snippet
    return sql.SQL("{}={}").format(colname, value)


def delete_snip(table: str, where: dict):
    """Base sql snippet for delete().

    Args:
        table (str): database table name
        where (dict): dict(colname=value) that 
        filters value to remove from table

    Returns:
        _type_: _description_
    """

    # Pass key-value tuple to compose_key_value()
    composed_where = map(compose_key_value, where.items())

    return sql.SQL("DELETE FROM {} WHERE {};").format(
        sql.Identifier(table),
        sql.SQL(" AND ").join(map(where_snip, composed_where)),
    )


if __name__ == "__main__":
    # dev testing, remove later
    import os
    from timeit import timeit
    # print("SNIPPET.PY")

    conn_import: dict = {
        "user": os.environ.get("PG_USER"),
        "password": os.environ.get("PG_PASSWORD"),
        "host": os.environ.get("PG_HOST"),
        "dbname": os.environ.get("PG_DBNAME"),
        "port": os.environ.get("PG_PORT"),
    }

    # Connect to an existing database
    with connect(**conn_import) as conn:

        dtest = {"name": "bos", "age": 34, "Date(ts)": "2022-04-19"}
        # dtest = {"name": "bos", "age": 34, "ts": "2022-04-19"}

        

        # tests
        # snipp = extract_sqlfunc_colname("Date(ts)")
        # snipp = create_unique_index("mytable", ["name", "Date(ts)"])
        
        # timeit(delete_snip, "mytable", dtest)
        # def a():
            # snipp = delete_snip("mytable", dtest)
            # print(snipp.as_string(conn))

        snipp = delete_snip("mytable", dtest)
        print(snipp.as_string(conn))

        # Code used to time speed of functions
        # print(timeit(stmt='delete_snip("mytable", dtest)', number=1000, globals=globals())/1000)
        # print(timeit(stmt='delete_snip("mytable", dtest)', setup='from __main__ import delete_snip, dtest', number=1))
        # print(timeit(stmt='a()', setup='from __main__ import a', number=100))
        

        conn.close()
