import os
from psycopg import connect
from datetime import datetime
from wrapg import wrapg
import common


# Note: to run pytests
# pipenv run python -m pytest
#  -m flag to run as module, all tests
#  -v flag to see verbose output
#  --capture=no flag to see print statements


# TODO: Need code to create new db and add table; if exist, ignore
# TODO: Setup test db .env variables
# conn_import: dict = {
#     "user": os.environ.get("PG_USER"),
#     "password": os.environ.get("PG_PASSWORD"),
#     "host": os.environ.get("PG_HOST"),
#     "dbname": os.environ.get("PG_DBNAME"),
#     "port": os.environ.get("PG_PORT"),
# }

test_table = os.environ.get("TEST_TABLE")


def test_insert():

    # ================================================
    #                Insert()
    #
    # - insert list of dictionaries
    #  TODO: add dataframe insert test
    # - test checks if Ethan record is UPDATED to 'Captain America'
    # - test checks if Matthew record is INSERTED
    # ================================================

    data = [
        {
            "age": 4,
            "superhero": "Captain America",
            "bike": "Speed Bike",
            "name": "Ethan",
            "ts": datetime(2022, 1, 1, 7, 0),
        },
        {
            "age": 33,
            "bike": "Road Bike",
            "name": "Matthew",
            "superhero": "Iron Man",
            "ts": datetime(2022, 4, 1, 7, 0),
        },
    ]

    wrapg.insert(data=data, table=test_table)

    qry = f"SELECT * FROM {test_table}"
    result = wrapg.query(raw_sql=qry)
    records = list(result)
    # print(records)

    # check insert()
    assert records == data


def test_update():

    # ================================================
    #              Update()

    # - uses sql type case function in key
    # - test checks if all data is updated
    # ================================================

    ethan_data = {
        "age": 5,
        "superhero": "Bumble Bee",
        "bike": "Mountain Bike",
        "name": "Ethan",
        "ts": datetime(2022, 1, 1, 10, 0),
    }

    # Update() with sql type cast function
    wrapg.update(data=ethan_data, table=test_table, keys=["Date(ts)"])

    # query updated ethan record
    qry = f"SELECT * FROM {test_table} WHERE name='Ethan'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check updated ethan record
    assert record == ethan_data


def test_update_exclude():
    # ================================================
    #      Update() with exclude_data parameter
    #
    # - uses sql date() to type cast function in key
    # - age updated with new value while ts & superhero
    #   should not update
    # ================================================

    matthew_data = {
        "age": 3,
        "bike": "Road Bike",
        "name": "Matthew",
        "superhero": "Gold Spider",
        "ts": datetime(2022, 4, 1, 10, 0),
    }

    # Update() with exclude_update
    wrapg.update(
        data=matthew_data,
        table=test_table,
        keys=["Date(ts)"],
        exclude_update=["ts", "age"],
    )

    # retrieve updated mathew record
    qry = f"SELECT * FROM {test_table} WHERE name='Matthew'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check updated matthew record
    assert record["superhero"] == "Gold Spider"

    # Info Should not update
    assert record["age"] == 33
    assert record["ts"] == datetime(2022, 4, 1, 7, 0)


def test_clear_table():

    qry = f"SELECT * FROM {test_table}"
    result = wrapg.query(raw_sql=qry)

    if list(result):
        wrapg.clear_table(table=test_table)

        qry = f"SELECT * FROM {test_table}"
        result = wrapg.query(raw_sql=qry)

        # check if clear_table(); no records
        assert len(list(result)) == 0

    else:
        raise Exception("No records to run clear table on")
