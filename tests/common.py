from wrapg import wrapg
from typing import Iterable


def drop_index(table: str, keys: Iterable):
    """
    Drop index created by wrapg for specified table.
    index format: tablename_keys_uix

    Args:
        table (str): tablename
    """
    key_str = "_".join(keys)

    try:
        drop_indx = f'DROP INDEX "{table}_{key_str}_uix";'
        wrapg.query(raw_sql=drop_indx)
    except Exception as e:
        # print('error>>>>>: ', e)
        pass


def clear_table(table: str):
    """Clear all records in specified table.

    Args:
        table (str): _description_
    """

    wrapg.clear_table(table=table)
