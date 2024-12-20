# wrapg

Wrapper around [_psycopg 3.x_](https://www.psycopg.org/psycopg3/docs/index.html) meant to make easy use of postgres using simple python functions. Primary focus is processing python data structures into and out of postgres.

Project is in infancy, _work in progress_.

## Table of Contents

- [Features](#features)
- [Installing Wrapg](#setup)
- [Usage](#usage)
- [Todo](#todo)
- [Acknowledgements](#acknowledgements)
- [Contact](#contact)
- [License](#license)

## Features

- Simple python functions to run postgres sql via python. See [Usage](#usage) section for more details on list of functionality.
  - upsert & insert_ignore functions included
    - 'use_index=True' automatically creates index
    - 'use_index=False' to upsert without using index (slower)
  - copy_from_csv() function to follow postgres COPY protocol (today only csv is available)
- Pass/Retrieve various python data structutes via underlining sql functions
  - tuples
  - dictionaires
  - pandas dataframe
- Auto import default postgres connection parameters via .env
  - saves on repeating code to specify connection
  - overide default connection parameters with kwargs in each function if needed
- Use of postgres sql functions with certain parametes. _(work in progress, today mosty use Date() to type cast in keys parameters)_

## Installing Wrapg

Wrapg is available on PyPI:

```
pip install wrapg
```

Dependencies:

- python 3.10+
- [psycopg[binary]>=3.0.11+](https://www.psycopg.org/psycopg3/docs/index.html)
- [pandas>=1.4.2+](https://pandas.pydata.org/docs/index.html)

## Usage

### Connection Parameters

Before you get started is is recommended to create .env file in main project directory to store connection parameters.  
Wrapg will read the .env file via pipenv (future: will remove need for pipenv) and set default connection parameters.  
The .env file should contain below specific named variables that follow postgres connection parameter key words as shown below.

```
# Database connection parameters
host=localhost
port=1234
dbname=supers
user=postgres
password=mypass
```

> _Any connection parameter can be overwritten via `conn_kwargs` in each function per [postgres connection parameter key words](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)._

Below example shows how the default .env specified `dbname=supers` was changed to `dbname=sales`.

```
qry="SELECT * FROM customers"

wrapg.query(raw_sql=qry, conn_kwargs={'dbname': 'sales'})

```

### Create Database

Function to create database.

```
wrapg.create_database(name="supers")
```

> Note: `dbname` will be silenced in connection string if set via .env file.

### Create Table

Function to help create table.

```
cols = dict(id="serial PRIMARY KEY", name="varchar(75) unique not null", age="int")

wrapg.create_table(table="villian", columns=cols)
```

### Insert

Insert function using list of dictionaries or a pandas dataframe.

```
info = [{'name': 'Peter Paker', superhero: 'Spider-man', 'email': 'webhead@gmail.com'},
{'name': 'Bruce Wayne', superhero: 'Batman', 'email': 'bwayne@gmail.com'}]

wrapg.insert(data=info, table="superhero")
```

### Update

Easily call sql update.

- If rows with matching keys exist, update row values.
- The columns/info that is not provided in the 'data' retain their original values.
- keys parameter must be specified in function.

```
new_email = {superhero: 'Spider-man', 'email': 'spidey@gmail.com'}

wrapg.update(data=new_email, table="superhero", keys=["superhero"])
```

### Upsert

Easily call sql upsert.

- Add a row into specified table if the row with specified keys does not already exist.
  - If rows with matching keys parameter exist, update row values.
- Automatically creates unique index if one does not exist for keys provided when use_index=True (Default)
  - If use_index=False, auto creation of index will not occur and operation will first try to update record, then insert (slower)

```
record = {'name': 'Steve Rogers', superhero: 'Captian America', 'email': 'cap@gmail.com'}

wrapg.upsert(data=record, table="superhero", keys=["email"], use_index=True)
```

### Insert Ignore

Easily call sql insert ignore.

- Add a row into specified table if the row with specified keys does not already exist.
  - If rows with matching keys exist no change is made.
- Automatically creates unique index if one does not exist for keys provided.
  - _(Future give option to turn off auto index)_

```
record = {'name': 'Dr Victor von Doom', villian: 'Dr Doom', 'email': 'doom@gmail.com'}

wrapg.insert_ignore(data=record, table="villian", keys=["email"], use_index=True)
```

### Copy from CSV

Calls sql copy.

- Specify db table and csv file
- header boolean paramenter available and csv read block size

```
wrapg.copy_from_csv(table="heroes", csv_file='hero.csv', header=True)
```

### Query

For more complicated sql not covered by a specific function, one can use query() function to pass raw sql.

```
qry="SELECT COUNT(DISTINCT alarm), locid, "Date" FROM metrics
WHERE "Date"='2020-08-02'
GROUP BY locid
ORDER BY COUNT(alarm) DESC"

wrapg.query(raw_sql=qry)
```

```
qry_with_params = "INSERT INTO some_table (id, created_at, updated_at, last_name)
VALUES (%(id)s, %(created)s, %(created)s, %(name)s);"


info = {'id': 10, 'name': "O'Reilly", 'created': datetime.date(2020, 11, 18)}

wrapg.query(raw_sql=qry, params = info)

```

## Todo

[x] Changed .env connection parameters to match postgres sql connection parameter names (11/16/24)  
[x] Add params parameter to .query() function; allows to pass named & un-named placeholders in queries (11/17/24)  
[ ] Add code to query() func to allow executemany() for Iterable[data]? what scenerio is this needed?  
[ ] \*\*Create session instance to run mutliple function within same session; how will this affect conn_kwargs for operations on different dbs  
[ ] \*\*handle env better without pipenv?  
[ ] \*Return scalar from query function vs iter[dict]/df (apply to applicable funcs); return_type parameter?  
[ ] \*Ability to pass iter[dict] to funcs like insert; read util functions  
[ ] Table manupulation drop_column(), drop-table(), add_column(), delete_table()  
[ ] Add copy to (data from table to file)  
[ ] use polars? for better performance and memory managment  
[ ] Add ability to convert column to ['identity'](https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-identity-column/) column with start, increment attribute  
[ ] insert_ignore() without index  
[ ] Handle other operators other than '='; >, <, <>, in, between, like?  
[ ] Implement create_index(), distinct(), drop_index()  
[ ] Handle JSON, ITERATOR?  
[ ] \*\*Add more tests

## Acknowledgements

This project built on great work by [psycopg 3](https://www.psycopg.org/psycopg3/docs/index.html) and was inspired by [_dataset_](https://dataset.readthedocs.io/en/latest/)

## Contact

Wrapped by [_jturnercode_](https://github.com/jturnercode)

## License

- MIT
