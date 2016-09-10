![](http://www.blazingdb.com/images/Logo_Blazing.png)



## Intro

[blazingdb.com](http://www.blazingdb.com) python connector to BlazingDB, a high performance DB with petabyte scaling.
BlazingDB is an extremely fast SQL database able to handle petabyte scale.

See [BlazingDB docs] (https://blazingdb.readme.io/docs/quickstart-guide-to-blazingdb)

## Installation

`pip install blazingdb`



## Usage

###### Verify example

```py
>>> from blazingdb import BlazingPyConnector
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name')
>>> con = bl.connect()
>>> result = bl.run("list tables",con)
>>> print result.status
>>> print result.rows
```

## Author

BlazingDB Inc. ([www.blazingdb.com](http://www.blazingdb.com))


## Deploy

```
python setup.py sdist upload -r pypi
```
