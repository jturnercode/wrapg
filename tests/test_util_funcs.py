from wrapg import util

# Note: run test using -m flag
# pipenv run python -m pytest -vv


def test_uniform_dict_keys():
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
    assert util.uniform_dict_keys(data1) != 1
    assert util.uniform_dict_keys(data2) == 1


def test_check_all_dicts():
    not_uniform = (
        {"num": 30, "data": "thirty", "bike": "brown", "name": "Pete"},
        {"num": 80, "data": "eighty", "name": "Red"},
        (1, 2, 3),
    )

    uniform = (
        {"num": 30, "data": "thirty", "bike": "brown", "name": "Matthew"},
        {"num": 80, "bike": "Red", "name": "Ethan", "data": "eighty"},
    )

    # data_transform() returns (column names, )
    # test returned columns
    # TODO: How do i check BaseException?
    # assert util.check_all_dicts(not_uniform) == 'BaseException: Iterable has mixed types, expected Iterable[dictionaries]'
    assert util.check_all_dicts(uniform) is True


def test_data_transform():
    tup_dict = (
        {"num": 30, "data": "thirty", "bike": "brown", "name": "Pete"},
        {"num": 80, "data": "eighty", "name": "Red"},
    )

    test_dict = {"num": 80, "data": "eighty", "name": "Red"}

    # data_transform() returns (column names, data, uniform)
    # test returned columns
    assert util.data_transform(tup_dict) == (tuple(tup_dict[0]), tup_dict, 2)
    assert util.data_transform(test_dict) == (tuple(test_dict), (test_dict,), 1)
    # TODO: Check more data structures
