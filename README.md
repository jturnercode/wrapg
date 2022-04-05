# wrapg
Wrapper around [_psycopg 3.x_](https://www.psycopg.org/psycopg3/docs/index.html) meant to make easy use of postgres within scripts using simple functions.

Project is in infancy, _work in progress_.

## Table of Contents
* [Features](#features)
* [Setup](#setup)
* [Usage](#usage)
* [Todo](#todo)
* [Acknowledgements](#acknowledgements)
* [Contact](#contact)
* [License](#license)


## Features
- Simple functions to run postgres sql via python
- Pass various data structutes into sql functions
    - supports pandas dataframe out of box
- Auto import default postgres connection parameters via .env
    - saves on repeating code to specify connection
    - overide default connection parameters with kwargs in each function
    - see examples for more details *(future)*
- Upsert, insert_ignore functions included
    - automatically creates index
    - option to upsert/insert_ignore without creating index (future)
- Copy functions to follow postgres COPY protocol *(today only csv is avail)*

## Setup
Dependencies:
- python 3.10+
- [psycopg[binary]>=3.0.11](https://www.psycopg.org/psycopg3/docs/index.html)
- [pandas>=1.4.2](https://pandas.pydata.org/docs/index.html)


## Usage
- TODO: add examples later


## Todo
- Add delete funtionality
- Handle JSON, ITERATOR?
- Add tests
- Make into package on pypi
- Optimize code after it is all working


## Acknowledgements
This project built on great work by [psycopg 3](https://www.psycopg.org/psycopg3/docs/index.html) and was inspired by [_dataset_](https://dataset.readthedocs.io/en/latest/) 


## Contact
Created by [_jturnercode_](https://github.com/jturnercode) - hope package is helps.

## License
- MIT


