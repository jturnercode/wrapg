import os
from datetime import datetime
from wrapg import wrapg
import common


test_table = os.environ.get("TEST_TABLE")


def test_upsert_noindex():

    # ================================================
    #              Upsert() no index
    #
    # - uses sql date() type cast function in key
    # ================================================

    # clear index specified index and table records
    common.drop_index(test_table, keys=["Date(ts)"])
    common.clear_table(test_table)

    insert_data = [
        {
            "age": 3,
            "superhero": "Spider-man",
            "bike": "BMX",
            "name": "Matthew",
            "ts": datetime(2022, 4, 1, 7, 0),
        }
    ]

    # first insert data to check update in upsert works properly
    wrapg.insert(data=insert_data, table=test_table)

    upsert_data = [
        {
            "age": 7,
            "superhero": "Gold Spider",
            "bike": "BMX",
            "name": "Matthew",
            "ts": datetime(2022, 4, 1, 11, 0),
        },
        {
            "age": 100,
            "bike": "New Bike",
            "name": "James",
            "superhero": "Batman",
            "ts": datetime(2022, 8, 1, 8, 0),
        },
    ]

    # data to upsert using date(ts) key
    wrapg.upsert(data=upsert_data, table=test_table, keys=["Date(ts)"], use_index=False)

    # check updated matthew record
    qry = f"SELECT * FROM {test_table} WHERE name='Matthew'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check update in upsert()
    assert record == upsert_data[0]

    # check inserted james record
    qry = f"SELECT * FROM {test_table} WHERE name='James'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check insert in upsert()
    assert record == upsert_data[1]


def test_upsert_index():

    # ================================================
    #              Upsert() with index
    #
    # - uses sql type case function in key
    # ================================================

    # clear index specified index and table records
    common.drop_index(test_table, keys=["Date(ts)"])
    common.clear_table(test_table)

    insert_data = [
        {
            "age": 3,
            "superhero": "Spider-man",
            "bike": "BMX",
            "name": "Matthew",
            "ts": datetime(2022, 4, 1, 7, 0),
        }
    ]

    # insert test data
    wrapg.insert(data=insert_data, table=test_table)

    upsert_data = [
        {
            "age": 7,
            "superhero": "Gold Spider",
            "bike": "BMX",
            "name": "Matthew",
            "ts": datetime(2022, 4, 1, 11, 0),
        },
        {
            "age": 100,
            "bike": "New Bike",
            "name": "James",
            "superhero": "Batman",
            "ts": datetime(2022, 8, 1, 8, 0),
        },
    ]

    # data to upsert using date(ts) key
    wrapg.upsert(data=upsert_data, table=test_table, keys=["Date(ts)"], use_index=True)

    # check updated matthew record
    qry = f"SELECT * FROM {test_table} WHERE name='Matthew'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check update in upsert()
    assert record == upsert_data[0]

    # check inserted james record
    qry = f"SELECT * FROM {test_table} WHERE name='James'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check insert in upsert()
    assert record == upsert_data[1]

    # clean up for other tests
    common.drop_index(test_table, keys=["Date(ts)"])
    common.clear_table(test_table)
