import re
from typing import Iterable
from psycopg import sql, connect


# Regex to seperate sql_func from column name
__compiled_pattern = re.compile(pattern=r"(\w*)\((\w*)\)")


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


def colname_snip(sqlfunc_colname: tuple):
    """Return escaped sql snippet, accomodate column names
    wrapped by sql functions.

    Args:
        column_detail (str | tuple): column_name as str
        or tuple (sql_func, column_name)

    Returns:
        Composed: snippet of sql statment
    """
    sqlfunc, colname = sqlfunc_colname

    # return snippet of sql func wrapping column name
    if sqlfunc is None:
        # return escaped column name
        return sql.SQL("{}").format(
            sql.Identifier(colname),
        )

    return sql.SQL("{}({})").format(
        sql.SQL(sqlfunc),
        sql.Identifier(colname),
    )


# =================== Unique Index Snippet ===================


def create_unique_index(table, keys):

    # Note name will include parenthsis if passed
    # unique index name
    uix_name = f'{table}_{"_".join(keys)}_uix'

    # If sql function in the any key
    if check_for_func(keys):

        # Return tuple of (sql func, colname)
        sqlfunc_keys = map(get_sqlfunc_colname, keys)

        # sql snippet to create unique index
        return sql.SQL("CREATE UNIQUE INDEX {} ON {} ({});").format(
            sql.Identifier(uix_name),
            sql.Identifier(table),
            sql.SQL(", ").join(map(colname_snip, sqlfunc_keys)),
        )

    # Sql snippet to create unique index
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


def upsert_snip(table: str, columns: Iterable, keys: Iterable):

    # if sql function in the any key
    if check_for_func(keys):

        # Return tuple of (sql func, key)
        sqlfunc_keys = map(get_sqlfunc_colname, keys)

        # Sql snippet to upsert
        return sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {};"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # conflict target
            sql.SQL(", ").join(map(colname_snip, sqlfunc_keys)),
            # set new values
            sql.SQL(", ").join(map(exclude_sql, columns)),
        )

    # Sql snippet to upsert
    return sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {};"
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

    # If sql function in the any key
    if check_for_func(keys):

        # Return tuple (sql func, key)
        sqlfunc_keys = map(get_sqlfunc_colname, keys)

        # Sql snippet to insert ignore
        return sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # conflict target
            sql.SQL(", ").join(map(colname_snip, sqlfunc_keys)),
        )

    # Sql snippet to insert ignore
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
        # pattern = r"(\w*)\((\w*)\)"
        result = re.search(__compiled_pattern, colname)

        # Extract matching values of all groups
        # this is done to escape column name
        sqlfunc = result.group(1).upper()
        column = result.group(2)

        composed_column = sql.SQL("{}({})").format(
            sql.SQL(sqlfunc), sql.Identifier(column)
        )

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


# =================== Update Snippets ===================


def get_sqlfunc_colname(colname: str):
    """
    Extract sql function from column name.
    Return (sql function, column name)
    If no sql function return None in tuple.
    Used on Iterable of strings via map().

    Args:
        colname (str): column name

    Returns:
        tuple: (None, colname) or (sqlfunc, colname)
    """
    # Check if colname has a function
    if "(" in colname:
        try:
            # pattern = r"(\w*)\((\w*)\)"
            result = re.search(__compiled_pattern, colname)

            # Extract matching values of all groups
            # this is done to escape column name
            sqlfunc = result.group(1).upper()
            colname = result.group(2)

            return sqlfunc, colname
        except:
            print("Invalid column name passed!")
    return None, colname


# function used to map to column names
def colname_placeholder_snip(sqlfunc_colname: tuple):

    """Return sql snip for where and set clauses with
    placeholder for value;
    ex. "SET col=value, col2=value2
    colname=%(colname)s or sqlfun(colname)=%(colname)s

    Args:
        sqlfunc_colname (tuple): (sqlfunc, colname)

    Returns:
        Composable: colname=%(colname)s
    """
    sqlfunc, colname = sqlfunc_colname
    if sqlfunc is None:
        return sql.SQL("{}={}").format(
            sql.Identifier(colname),
            sql.Placeholder(colname),
        )

    return sql.SQL("{}({})={}").format(
        sql.SQL(sqlfunc),
        sql.Identifier(colname),
        sql.Placeholder(colname),
    )


def update_snip(table: str, columns: Iterable, keys: Iterable):
    columns = map(get_sqlfunc_colname, columns)
    keys = map(get_sqlfunc_colname, keys)

    return sql.SQL("UPDATE {} SET {} WHERE {}").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(colname_placeholder_snip, columns)),
        sql.SQL(" AND ").join(map(colname_placeholder_snip, keys)),
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
        # snipp = create_unique_index("mytable", ["name", "Date(ts)"])

        # timeit(delete_snip, "mytable", dtest)
        # def a():
        # snipp = delete_snip("mytable", dtest)
        # print(snipp.as_string(conn))

        # snipp = delete_snip("mytable", dtest)

        snipp = update_snip(
            table="mytable", columns=["a", "b", "c"], keys=["Date(b)", "d"]
        )
        print(snipp.as_string(conn))

        # Code used to time speed of functions
        # print(timeit(stmt='delete_snip("mytable", dtest)', number=1000, globals=globals())/1000)
        # print(timeit(stmt='delete_snip("mytable", dtest)', setup='from __main__ import delete_snip, dtest', number=1))
        # print(timeit(stmt='a()', setup='from __main__ import a', number=100))

        conn.close()
