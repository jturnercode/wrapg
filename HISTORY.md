# Release History


0.2.1 (2022-04-21)
-------------------
**Improvements**
- Add code for delete() which takes dictionary for where clause parmeters
    - The column name can be wrapped with sql function like upsert and update functions
- Add clear_table() to quickly clear all data in specified table


0.2.0 (2022-04-13)
-------------------
**Improvements**
- Add code to accept sql functions on column keys for upsert() and ignore_insert() functions.
ie. upsert(keys=['name', 'Date(ts_column)'])
- Change .env PG_USERNAME variable to PG_USER to better match postgres user connection parameter.
- Added some examples to readme.md

**Bugfixes**
- Escape create_table() column names


0.1.4 (2022-04-11)
-------------------
**Improvements**


**Bugfixes**
- Upsert() not working due to bad code.