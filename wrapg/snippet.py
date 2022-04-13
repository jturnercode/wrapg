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
        func = column_name.replace(column + ")", "").capitalize()
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

    # else return snippet if sql func wrapping column
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


def upsert_snip(table, columns, keys):

    # if sql function in the any key
    if check_for_func(keys):

        # Return tuple of functions and key else just key
        seperated_keys = map(extract_sqlfunc_colname, keys)

        return sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Placeholder, columns)),
            # conflict target
            sql.SQL(", ").join(map(colname_snip, seperated_keys)),
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


def insert_ignore_snip(table, columns, keys):

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


if __name__ == "__main__":
    # dev testing, remove later
    import os

    print("SNIPPET.PY")

    conn_import: dict = {
        "user": os.environ.get("PG_USERNAME"),
        "password": os.environ.get("PG_PASSWORD"),
        "host": os.environ.get("PG_HOST"),
        "dbname": os.environ.get("PG_DBNAME"),
        "port": os.environ.get("PG_PORT"),
    }

    # Connect to an existing database
    with connect(**conn_import) as conn:

        # test
        snippet = extract_sqlfunc_colname("Date(ts)")

        snippet = create_unique_index("mytable", ["name", "Date(ts)"])
        print(snippet.as_string(conn))

        conn.close()
