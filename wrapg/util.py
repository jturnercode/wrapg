from collections.abc import Iterable
import pandas as pd
from numpy import nan


def check_all_dicts(iterable_dict: Iterable[dict]):
    """Check if Iterable contains all dictionaries

    Args:
        iterable_dict (Iterable[dict]): Iterable of dictionaries
    """
    # Check if dict
    def check_dict(d):
        return isinstance(d, dict)

    # Check if all instances are type dict, return True or False
    all_dict = all(map(check_dict, iterable_dict))
    # print(all_dict)

    if not all_dict:
        raise BaseException("Iterable has mixed types, expected Iterable[dictionaries]")

    return True


def uniform_dict_keys(iterable_dict: Iterable[dict]) -> int:
    """
    Function evaluates if Iterable of dictionaries
    has same keys for each dictionary.
    If true, this will allow for one query
    to process data. ie one query to insert/update/etc
    all rows vs having to create a query
    for each dictionary (non uniform key/pair data)

    Args:
        iter_dict (_type_): Iterable of dictionaries
        representing data to be processed into database

    Return:
     Length of unique sets of keys. If returns 1,
     then this indicates all dicts in list
     have the same keys (uniform)
    """

    def key_sort(dict) -> tuple:
        """Return sorted dictionary keys for each dict"""
        # sorted(dict) returns keys
        # Need a tuple to process set()
        return tuple(sorted(dict))

    # Compare all tuples of keys and apply set() to identify unique key combinations
    # If len(set) = 1, then all keys same for all dictionaries->uniform data structure
    keys = set(map(key_sort, iterable_dict))

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
            # in case a df with nan is passed, None needed for sql
            df = data_structure.replace(nan, None)
            # returns list of dictionaries
            rows = df.to_dict(orient="records")
            uniform = 1

            # print(rows)
            return columns, rows, uniform

        case list() | tuple():
            # print("type -> list/tuple of dictionaries")

            # =========== iterable[dict] to df not used============
            # !ISSUE: Convert dict to df may update some columns
            # !for non-uniform case to None, unintentionally
            # !(non-uniform keys in all dicts to df)
            # df = pd.DataFrame(data_structure)
            # columns = df.columns
            # FIXME: below will convert intergers to float if column has nan, fix!
            # df = df.replace(nan, None)
            # rows = list(df.itertuples(index=False, name=None))

            # Check all instances in data structure are dictionaires
            if check_all_dicts(data_structure):

                # Return tuple(dict.keys of first instance in Iterable)
                # If not uniform program will iterate over instance
                # if uniform, checking first instance is good enough
                columns = tuple(data_structure[0])

                # Check if all dictionaries have same keys
                uniform = uniform_dict_keys(data_structure)

                return columns, data_structure, uniform

        case dict():
            # print("type -> dictionary")

            # return tuple (not used for now)
            # rows = [tuple(data_structure.values())]

            # return keys of dict in a tuple
            columns = tuple(data_structure)
            # tuple of one dictionary
            row = (data_structure,)
            uniform = 1

            return columns, row, uniform

        case []:
            raise ValueError(
                f"Empty list passed, must contain at least one dictionary."
            )

        case _:
            raise ValueError(f"Unsupported data structure passed.")



def iterable_difference(minuend: Iterable, subtrahend: Iterable) -> tuple:
    """Used to get the difference between two iterables or sequences.
    ie minuend - subtrahend = difference

    Args:
        minuend (Iterable): _description_
        subtrahend (Iterable): _description_
    """


    return tuple(m for m in minuend if m not in subtrahend)