from collections.abc import Iterable
import pandas as pd
from numpy import nan


def uniform_data(iter_dict: Iterable[dict]) -> int:
    """
    Function evaluates if list of dictionaries
    has uniform keys for each dictionary.
    If true, this will allow for one query
    to process data. ie one query to insert/update/etc
    many rows vs having to create a query
    for each dictionary (non uniform key/pair data)

    Args:
        iter_dict (_type_): Iterable of dictionarie
        representing data to be processed into database

    Return:
     Length of unique sets of keys. If returns 1,
     then this indicates all dicts in list
     have the same keys (uniform)
    """
    # Check if all instances are type dict
    def check_alldict(d):
        return isinstance(d, dict)

    all_dict = all(map(check_alldict, iter_dict))
    # print(all_dict)

    if not all_dict:
        raise BaseException("Iterable has mixed types, expected all dictionaries")

    def tup_sort(dict) -> tuple:
        """Return sorted tuple of keys for each dict"""
        # sorted(dict) returns keys
        # need tuple to process set()
        return tuple(sorted(dict))

    keys = set(map(tup_sort, iter_dict))

    return len(keys)


def data_transform(data_structure):
    """Internal function checks passed data structure and
    returns tuple of columns and Iterable of rows(dictionaries)

    Args:
        data_structure (Any): data needing to be inserted/
        updated/etc into postgres (type: dataframe,
        list/tuple of dict, dict)

    Returns:
        column, row, uniform: tuple(column_names), Iterable(dict), int
        uniform = 1 indicates all dictionaries have same keys
    """

    # =================== TODO ===================
    # TODO: handle json data, named tuple?
    # TODO: handle iterator?
    # TODO: test if returning rows of tuples faster to process?

    # structural pattern matching for data_structure passed
    match data_structure:
        case pd.DataFrame():
            """
            Dataframe is a uniform data structure, no varying
            columns for each row; missing values are converted
            to None for postgres
            """
            # print("type -> dataframe")

            # =============== df to tuple(not used) ===============
            # Need to replace all NaN to None, pg sees nan as str
            # df = data_structure.replace(nan, None)
            # # return named tuple
            # rows = tuple(df.itertuples(index=False, name=None))

            columns = tuple(data_structure.columns)
            df = data_structure.replace(nan, None)
            rows = df.to_dict(orient="records")
            uniform = 1

            # print(rows)
            return columns, rows, uniform

        case list() | tuple() | set():
            # print("type -> list/tuple of dictionaries")

            # =========== iterable[dict] to iterable(tuple) not used ============
            # TODO: improve? dataframes with large datasets, CHUNK?
            # Pass data into dataframe to make sure it is in
            # !consistent order and account for non-uniform data
            # This allows for one dynamic query vs having to
            # recreate query for each row(dictionary)
            # !ISSUE: Convert dict to df may update some columns
            # !for non-uniform case to None, unintentionally
            # !(non-uniform keys in all dicts to df)
            # df = pd.DataFrame(data_structure)
            # columns = df.columns
            # FIXME: below will convert intergers to float if column has nan, fix!
            # df = df.replace(nan, None)
            # rows = list(df.itertuples(index=False, name=None))

            # return tuple(dict.keys of first instance in Iterable)
            columns = tuple(data_structure[0])
            uniform = uniform_data(data_structure)

            return columns, data_structure, uniform

        case dict():
            # print("type -> dictionary")

            # return tuple (not used for now)
            # rows = [tuple(data_structure.values())]

            # return keys of dict in a tuple
            columns = tuple(data_structure)
            uniform = 1

            return columns, (data_structure), uniform

        case []:
            raise ValueError(
                f"Empty list passed, must contain at least one dictionary."
            )

        case _:
            raise ValueError(f"Unsupported data structure passed.")
