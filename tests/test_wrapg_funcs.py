import os
from psycopg import connect
from wrapg import wrapg
from datetime import datetime


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

table = "test3"


def test_clear_table():

    wrapg.clear_table(table=table)

    qry = f"SELECT * FROM {table}"
    result = wrapg.query(raw_sql=qry)

    # check if clear_table(); no records
    assert len(list(result)) == 0


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
            "num": 300,
            "superhero": "Captian",
            "bike": "Huffy",
            "name": "Ethan",
            "ts": datetime(2022, 5, 5, 8, 0),
        },
        {
            "num": 900,
            "superhero": "Iron Man",
            "bike": "Honda",
            "name": "James",
            "ts": datetime(2022, 1, 1, 1, 1),
        },
    ]

    wrapg.insert(data=data, table=table)

    qry = f"SELECT * FROM {table} WHERE name='Ethan'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]
    del record["id"]
    # print(record)

    # check insert()
    assert record == {
        "num": 300,
        "superhero": "Captian",
        "bike": "Huffy",
        "name": "Ethan",
        "ts": datetime(2022, 5, 5, 8, 0),
    }


def test_upsert_noindex():

    # ================================================
    #              Upsert() no index
    #
    # - uses sql type case function in key
    # - test checks if Ethan record is UPDATED to 'Captain America'
    # - test checks if Matthew record is INSERTED
    # ================================================

    data = [
        {
            "num": 300,
            "superhero": "Captain America",
            "bike": "Huffy",
            "name": "Ethan",
            "ts": datetime(2022, 5, 5, 9, 0),
        },
        {
            "num": 200,
            "bike": "BMX",
            "name": "Matthew",
            "superhero": "Spider-man",
            "ts": datetime(2022, 8, 15, 13, 0),
        },
    ]

    # upsert data
    wrapg.upsert(data=data, table=table, keys=["Date(ts)"], use_index=False)

    # check updated ethan record
    qry = f"SELECT * FROM {table} WHERE name='Ethan'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check update in upsert()
    assert record["superhero"] == "Captain America"

    # check inserted matthew record
    qry = f"SELECT * FROM {table} WHERE name='Matthew'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check insert in upsert()
    assert record["superhero"] == "Spider-man"


def test_upsert_index():

    # ================================================
    #              Upsert() with index
    #
    # - uses sql type case function in key
    # - test checks if James record is UPDATED
    # - test checks if Janie record is INSERTED
    # ================================================

    data = [
        {
            "num": 700,
            "superhero": "Hulk",
            "bike": "Honda CBR",
            "name": "James",
            "ts": datetime(2022, 7, 7, 7, 7),
        },
        {
            "num": 500,
            "bike": "Speed Bike",
            "name": "Janie",
            "superhero": "Black Widow",
            "ts": datetime(2022, 2, 14, 14, 14),
        },
    ]

    # upsert data
    wrapg.upsert(data=data, table=table, keys=["name"], use_index=True)

    # check updated james record
    qry = f"SELECT * FROM {table} WHERE name='James'"
    result = wrapg.query(raw_sql=qry)
    james_record = list(result)[0]
    del james_record["id"]

    # check update in upsert()
    assert james_record == data[0]

    # check inserted janie record
    qry = f"SELECT * FROM {table} WHERE name='Janie'"
    result = wrapg.query(raw_sql=qry)
    janie_record = list(result)[0]
    del janie_record['id']
    # check insert in upsert()
    assert janie_record == data[1]


def test_update():

    # ================================================
    #              Update()
    #
    # - uses sql type case function in key
    # - test checks if all data is updated
    # ================================================

    ethan_data = {
        "num": 333,
        "superhero": "Captain America",
        "bike": "Mountain",
        "name": "Ethan",
        "ts": datetime(2022, 5, 5, 5, 5),
    }

    # Update() with sql type cast function
    wrapg.update(data=ethan_data, table=table, keys=["Date(ts)"])

    qry = f"SELECT * FROM {table} WHERE name='Ethan'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]
    del record["id"]

    # check update in upsert()
    assert record == ethan_data

    # ================================================
    #      Update() with exclude_data parameter
    #
    # - uses sql type case function in key
    # - num updated with new value while ts & superhero
    #   should not update
    # ================================================

    matthew_data = {
        "num": 222,
        "bike": "BMX",
        "name": "Matthew",
        "superhero": "Gold Spider",
        "ts": datetime(2022, 8, 15, 16, 16),
    }

    # Update() with exclude_update
    wrapg.update(
        data=matthew_data,
        table=table,
        keys=["Date(ts)"],
        exclude_update=["ts", "superhero"],
    )

    # retrieve updated record
    qry = f"SELECT * FROM {table} WHERE name='Matthew'"
    result = wrapg.query(raw_sql=qry)
    record = list(result)[0]

    # check insert in upsert()
    assert record["superhero"] == "Spider-man"
    assert record["num"] == 222
