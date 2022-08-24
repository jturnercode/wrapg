# Chanelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.5] - 2022-08-xx
-------------------
### Added
- Added common.py in tests

### Changes
- Broke some more complex tests like for upsert() to specific files for better organization.

### Fixed
- Reverted code for colname_snip() as it was not allowing for auto-index creation. '::' is not allowed in index name.


## [0.2.4] - 2022-08-19
-------------------
### Fixed
- Fix snippet module refernce to iterable_difference. from wrapg import util vs from util import iterable_difference.


## [0.2.3] - 2022-08-14
-------------------
### Added
- Add more tests for wrapg functions
- Add exclude_update parameter to update() and upsert() functions

### Fixed
- Fix colname_snip() to properly cast types if date() passed; it was affecting upsert with index=False
- Fixed exisitng snippet tests due to type casting issue on colname_snip()


## [0.2.2] - 2022-05-07
-------------------
### Added
- Add insert_snip() to resuse insert code
- Improve regex to better detect functions in column
- Add 'use_index' to upsert(); set to False to upsert without use of unique index
- Add more tests on util.py and snippet.py

### Fixed
- Fix sqlfunc() to properly cast a timestamp to date()
- Fix update() with ' AND '


## [0.2.1] - 2022-04-21
-------------------
### Added
- Add code for delete() which takes dictionary for where clause parmeters
    - The column name can be wrapped with sql function like upsert and update functions
- Add clear_table() to quickly clear all data in specified table


## [0.2.0] - 2022-04-13
-------------------
### Added
- Add code to accept sql functions on column keys for upsert() and ignore_insert() functions.
ie. upsert(keys=['name', 'Date(ts_column)'])
- Change .env PG_USERNAME variable to PG_USER to better match postgres user connection parameter.
- Added some examples to readme.md

### Fixed
- Escape create_table() column names


## [0.1.4] - 2022-04-11
-------------------

### Fixed
- Upsert() not working due to bad code.