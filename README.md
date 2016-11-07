![](http://www.blazingdb.com/images/Logo_Blazing_verde.png)



## Intro

[blazingdb.com](http://www.blazingdb.com) python tools to BlazingDB, a high performance DB with petabyte scaling.
BlazingDB is an extremely fast SQL database able to handle petabyte scale.

The BlazingDB python tools contains a high performance connector and a migration tool at the moment supporting migrations from Redshift and Postgresql to BlazingDB.

See [BlazingDB docs] (https://blazingdb.readme.io/docs/quickstart-guide-to-blazingdb) for more information about its usage.

## Installation

`pip install blazingdb`



## Usage

###### Connection

```py
>>> from blazingdb import BlazingPyConnector
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name',port=8089)
>>> con = bl.connect()
>>> result = bl.run("list tables",con)
>>> print result.status
>>> print result.rows
```

###### File Importer
```py
>>> from blazingdb import BlazingPyConnector, BlazingImporter
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name',port=8089)
>>> importer = BlazingImporter(bl)
>>> importer.file_import (
>>>    files_path = '/directory/datasets/',                # Please complete this parameter to set the directory where the files to import are
>>>    columns = 'A string(20), B long, C double, D date',     # (Optional) Only set this parameter if you don't have the columns in the file
>>>    table = 'abc',                                          # (Optional) Only set this parameter if the table name is different from the file name
>>>    find_files = True,                                      # True by default, change to false if you want to load an specific file and the folder has more than one with the configured extension
>>>    files_extension = '.csv'                                # Please specify the extension of the files to be imported
>>> )
```

###### DB Migration

```py
>>> from blazingdb import BlazingPyConnector, BlazingETL
>>> import psycopg2 as postgres
>>> # Blazing Connection
>>> bl = BlazingPyConnector('127.0.0.1','user@domain.com','password','database_name',port=8089)
>>> con = bl.connect()
>>> # Postgresql Connection
>>> pg = postgres.connect(host='localhost',dbname='postgres',user='postgres',password='postgres')
>>> migrator = BlazingETL(pg, bl)
>>> migrator.migrate ( # Optional Parameters
>>>     create_tables=False,                                           # Specify if it's needed to create the tables in the migration
>>>     path='/datasets/',                                    		     # Specify where will the migration data files stored
>>>     blazing_path='/opt/blazing/disk1/blazing/blazing-uploads/',    # Specify in case it's wanted to copy the data to BlazingDB uploads folder
>>>     chunk_size=100000,                                             # Specify the size of the chunks of data to migrate
>>>     export_data_from_origin=True,                                  # Turn to False in case you already have the and only need to load them into BlazingDb
>>>     copy_data_to_blazing=False,                                    # Turn to True in case you want to copy the data to BlazingDB uploads folder
>>>     load_data_into_blazing=True,                                   # Turn to False in case you want to migrate only the structure and not the data
>>>     file_extension='.csv'                                          # Specify the data files extension
>>> )
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
