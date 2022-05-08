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
- Simple python functions to run postgres sql via python
- Pass/Retrieve various python data structutes via underlining sql functions
    - supports pandas dataframe out of box
- Auto import default postgres connection parameters via .env
    - saves on repeating code to specify connection
    - overide default connection parameters with kwargs in each function
    - see examples for more details
- Upsert, insert_ignore functions included
    - automatically creates index
    - pass sql functions on 'On Conflict' keys (see example)
    - use option 'use_index=False' to upsert without using index 
- Copy functions to follow postgres COPY protocol *(today only csv is avail)*


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
TODO: add more examples later

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
PG_DBNAME = "sales"
```

Any connection variable can be overwritten in each function call if needed per [postgres connection parameter key words](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

Below will overide connection dbname for specific function call.
```
qry="SELECT * FROM customer" 
wrapg.query(raw_sql=qry, conn_kwargs: {'dbname': 'anotherdb'})

```

### Upsert & Insert Ignore
Easily call sql upsert or insert_ignore.  
The 'keys' parameter represents the on conflict target & will create unique index automatically.
```
record = {'name': 'Dave', 'email': 'dave@email.com'}
wrapg.upsert(data=record, table="customer", keys=["email"])
```


## Todo
- Handle other operators other than '='; >, <, <>, in, between, like?
- insert_ignore() without index
- Implement create_index(), drop_column(), distinct()
- Handle JSON, ITERATOR?
- Add more tests
- Optimize code after it is all working


## Acknowledgements
This project built on great work by [psycopg 3](https://www.psycopg.org/psycopg3/docs/index.html) and was inspired by [_dataset_](https://dataset.readthedocs.io/en/latest/) 


## Contact
Created by [_jturnercode_](https://github.com/jturnercode) - hope package is helps.


## License
- MIT


