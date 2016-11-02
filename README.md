![](http://www.blazingdb.com/images/Logo_Blazing_verde.png)



## Intro

[blazingdb.com](http://www.blazingdb.com) python connector to BlazingDB, a high performance DB with petabyte scaling.
BlazingDB is an extremely fast SQL database able to handle petabyte scale.

See [BlazingDB docs] (https://blazingdb.readme.io/docs/quickstart-guide-to-blazingdb)

## Installation

`pip install blazingdb`



## Usage

###### Verify connection

```py
>>> from blazingdb import BlazingPyConnector
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name',port=8089)
>>> con = bl.connect()
>>> result = bl.run("list tables",con)
>>> print result.status
>>> print result.rows
```

###### Examples

```py
>>> from blazingdb import BlazingPyConnector
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name',port=8089)
>>> con = bl.connect()
>>> result = bl.run("create table my_new_table (field1 string(20), field2 long, field3 date, field4 double)",con)
>>> print result.status
```

## More SQL Docs

Please visit your [SQL Guide](https://blazingdb.readme.io/docs/blazingdb-sql-guide) for more information about query structures and examples.

## Author

BlazingDB Inc. ([www.blazingdb.com](http://www.blazingdb.com))


## Deploy

```
python setup.py sdist upload -r pypi
```
