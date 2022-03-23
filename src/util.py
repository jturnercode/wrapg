import pandas as pd
from numpy import nan


def uniform_data(list_dict: list[dict]) -> int:
    """
    Function evaluates if list of dictionaries
    has uniform keys for each dictionary.
    If true, this will allow for one query
    to process data. ie one query to insert/update/etc
     many rows vs having to create a query
     for each dictionary (non uniform key/pair data)

    Args:
        list_dict (_type_): list of dictionaries representing data to be processed into database

    Return:
     Length of unique sets of keys. If returns 1,
     then this indicates all dicts in list
     have the same keys (uniform)
    """

    def tup_sort(dict) -> tuple:
        """Return sorted tuple of keys for each dict"""
        # sorted(dict) returns keys
        # need tuple to process set()
        return tuple(sorted(dict))

    keys = set(map(tup_sort, list_dict))

    return len(keys)


def data_transform(data_structure):
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
        # TODO: This may be place for improvement if large datasets, chunk?
        case list(d) | tuple(d) if all(isinstance(x, dict) for x in d):
            # print("type -> list of dictionaries")
            # Pass data into dataframe to make sure it is in
            #! consistent order and account for non-uniform data
            # FIXME: Convert dict to df may update columns having data to None, unintentionally
            # This allows for one dynamic query vs having to
            # recreate query for each row(dictionary)
            df = pd.DataFrame(data_structure)
            columns = df.columns
            # FIXME: below will convert intergers to float if column has nan, fix!
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
