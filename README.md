# wrapg
Wrapper around [_psycopg 3.x_](https://www.psycopg.org/psycopg3/docs/index.html) meant to make easy use of postgres using simple python functions. Primary focus is processing python data structures into and out of postgres.

Project is in infancy, _work in progress_.


## Table of Contents
* [Features](#features)
* [Installing Wrapg](#setup)
* [Usage](#usage)
* [Todo](#todo)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
* [License](#license)


## Features
- Simple python functions to run postgres sql via python. see Usage section for more details on list of functionality.
    - upsert & insert_ignore functions included
        - 'use_index=True' automatically creates index
        - 'use_index=False' to upsert without using index (slower)
    - copy_from_csv() function to follow postgres COPY protocol *(today only csv is avail)*
- Pass/Retrieve various python data structutes via underlining sql functions
    - supports pandas dataframe out of box
- Auto import default postgres connection parameters via .env
    - saves on repeating code to specify connection
    - overide default connection parameters with kwargs in each function if needed
- Use of sql functions with certain parametes. *(work in progress, today mosty use Date() to type cast in keys parameters)*


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

### Connection
Before you get started is is recommended to create .env file with below connection parameters.
Wrapg will auto-import default connectionn parameters and make all functions ready to be executed.  
The .env file should contain below specific name variables
```
# Database config
PG_HOST = "localhost"
PG_USER = "postgres"
PG_PASSWORD = "mypass"
PG_PORT = 1234
PG_DBNAME = "supers"
```

> *Any connection variable can be overwritten in each function call if needed per [postgres connection parameter key words](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).*

Below will overide default PG_DBNAME from 'supers' database to the 'sales' database for specific function call.
```
qry="SELECT * FROM customers" 
wrapg.query(raw_sql=qry, conn_kwargs={'dbname': 'sales'})

```

### Create Table
Function to help create table from python.

```
cols = dict(id="serial", name="varchar(75) unique not null", age="int")
wrapg.create_table(table="villian", columns=cols)
```

### Insert
Insert function using list of dictionaries.  
*('info' can be in pandas dataframe form.)*
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
    - *(Future give option to turn off auto index)*

```
record = {'name': 'Dr Victor von Doom', villian: 'Dr Doom', 'email': 'doom@gmail.com'}
wrapg.insert_ignore(data=record, table="villian", keys=["email"], use_index=True)
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

## Todo
- Handle other operators other than '='; >, <, <>, in, between, like?
- insert_ignore() without index
- Implement create_index(), distinct(), drop_index()
- Table manupulation drop_column(), add_column(), delete_table()
- Handle JSON, ITERATOR?
- Add more tests
- Optimize code after it is all working


## Acknowledgements
This project built on great work by [psycopg 3](https://www.psycopg.org/psycopg3/docs/index.html) and was inspired by [_dataset_](https://dataset.readthedocs.io/en/latest/) 


## Contact
Wrapped by [_jturnercode_](https://github.com/jturnercode)


## License
- MIT


