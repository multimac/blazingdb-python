# BlazingDB Python Module

Contains classes for connecting to, and importing data from, BlazingDB.

---

## Connector

The Connector handles connecting to and running queries against BlazingDB.

**NOTE:** Currently there is a bug with BlazingDB where connection tokens are reset
after each successful query. Either call .connect() before each .query(), or pass
'auto_connect=True' into .query().

```python
import blazingdb

connector = blazingdb.Connector(
    host="localhost",
    user="blazing",
    password="password",

    # Configure the database to automatically connect to
    database=None,

    # Configure whether to use HTTPS when connecting
    https=True,

    # Configure the port to connect to BlazingDB over
    port=8443
)

# This can be skipped if you pass 'auto_connect=True' to .query()
connector.connect()

# Perform a query against BlazingDB
connector.query("SELECT TOP 1 * FROM table")
```

---

## Migrator

The Migrator class handles retrieving data from a Source and importing it into BlazingDB
via an Importer (see below for information on `Sources` and `Importers`)

```python
import blazingdb
import psycopg2

from blazingdb import importers, pipeline
from blazingdb.sources import postgres

# Create the connector to use when loading data into BlazingDB
connector = blazingdb.Connector(
    host="localhost",
    database="blazing",
    user="blazing",
    password="password"
)

# Create the source to use when retrieving data to load into BlazingDB
source = postgres.PostgresSource(
    psycopg2.connect(
        host="localhost",
        dbname="postgres"
        user="postgres",
        password="password",
    ),
    schema="default"
)

# Create any pipeline stages to use when processing tables into Blazing
stages = [
    pipeline.DropTableStage(),
    pipeline.CreateTableStage()
]

# Create the importer to use when loading data into BlazingDB
importer = importers.ChunkingImporter(
    "/path/to/blazing/uploads"
)

# Create the migrator using all the pieces above
migrator = blazingdb.Migrator(
    connector, source, stages, importer
)

# Import all tables into BlazingDB
migrator.migrate()

# Import multiple tables
migrator.migrate(["table-one", "table-two", "table-three"])

# Import only one table
migrator.migrate("table")
```

---

## Importers

Importers are means of loading data into BlazingDB. Depending on how much data you want to
load, `ChunkingImporter` may be faster than `StreamingImporter` because it isn't limited to 1MB
of data at a time.

```python
from blazingdb import importers

importer = importers.StreamingImporter(
    # Configure the size of each request in bytes
    chunk_size=1048576

    # Configure the encoding to use when calculating size of rows
    encoding="utf-8"

    # Configure the character to use when separating fields
    field_terminator="|"

    # Configure the character to use when wrapping fields
    field_wrapper="\""

    # Configure the character to use when separating rows
    line_terminator="\n"
)

connector = blazingdb.Connector(
    host="localhost",
    database="blazing",
    user="blazing",
    password="password"
)

# Importers can load any arbitrary iterable which returns arrays
importer.load(connector, "table", [
    ["a", 123, "2017-4-1"],
    ["b", 456, "1970-1-1"],
    ["z", 789, "1999-12-31"]
])
```

---

## Pipeline Stages

Pipeline stages are used to affect BlazingDB before/after tables have been imported.

At the moment, the following pipeline stages exist:
- CreateTableStage - Creates tables in BlazingDB before data is imported into them
- DropTableStage - Drops existing tables in BlazingDB before data is imported into them

---

## Sources

Sources are places where data you wish to import can originate from. Currently,
PostgreSQL (and inherently Amazon Redshift) is the only source available.

```python
import psycopg2
from blazingdb.sources import postgres

source = postgres.PostgresSource(
    psycopg2.connect(
        host="localhost",
        dbname="postgres"
        user="postgres",
        password="password",
    ),
    schema="default",

    # Configure the number of rows to retrieve at a time
    fetch_count=10000
)

# Retrieve list of tables
source.get_tables()

# Retrieve list of columns for a table
source.get_columns("table")

# Retrieve an iterable of all rows for a table
source.retrieve("table")
```