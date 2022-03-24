from wrapg import util

# run test using -m flag
# pipenv run python -m pytest


def test_uniform_data():
    data1 = (
        {"num": 30, "data": "thirty", "bike": "brown", "name": "pete"},
        {"num": 60, "data": "sixty", "bike": "green", "name": "joe"},
        {"num": 80, "data": "eighty", "bike": "red"},
        {"num": 90, "data": "ninety", "bike": "candy red", "name": "sr"},
    )

    data2 = (
        {"num": 30, "data": "thirty", "bike": "brown", "name": "pete"},
        {"num": 60, "data": "sixty", "bike": "green", "name": "joe"},
        {"num": 80, "data": "eighty", "bike": "red", "name": "burt"},
        {"num": 90, "data": "ninety", "bike": "candy red", "name": "sr"},
    )
    assert util.uniform_data(data1) != 1
    assert util.uniform_data(data2) == 1
