# BlazingDB Python Package

Contains classes for connecting to, and importing data from, BlazingDB.

---

## Connector

The Connector handles connecting to and running queries against BlazingDB.

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
    port=8443,

    # Restricts the number of concurrent requests to BlazingDB
    request_limit=5
)

# Perform a query against BlazingDB
connector.query("SELECT TOP 1 * FROM table")
```

---

## Importers

Importers are means of loading data into BlazingDB. Depending on how much data you want to
load, `ChunkingImporter` may be faster than `StreamingImporter` because of a 1MB limit on the
size of requests the `StreamingImporter` can make.

```python
import blazingdb

from blazingdb import importers
from blazingdb.importers import batchers
from blazingdb.pipeline import system
from blazingdb.sources import postgres

from datetime import date

batcher = batchers.ByteBatcher(
    # Configure the size of each request in bytes
    chunk_size=1048576,

    # Configure the encoding to use when calculating size of rows
    encoding="utf-8",

    # Configure how often progress should be logged (in seconds)
    log_interval=10
)

importer = importers.StreamingImporter(
    # Specify the batcher to use when generating requests to BlazingDB
    batcher=batcher,

    # Timeout (in seconds) for requests to BlazingDB
    timeout=None,

    # Configure the character to use when separating fields
    field_terminator="|",

    # Configure the character to use when wrapping fields
    field_wrapper="\"",

    # Configure the character to use when separating rows
    line_terminator="\n",

    # A configured pipeline to run before / after performing requests to BlazingDB
    # See the section on `Pipelines` below for more details
    pipeline=system.System()
)

connector = blazingdb.Connector(
    host="localhost",
    database="blazing",
    user="blazing",
    password="password"
)

source = postgres.PostgresSource(
    psycopg2.connect(
        host="localhost",
        dbname="postgres"
        user="postgres",
        password="password",
    ),
    schema="default",
)

# Importers take a dictionary of arguments when calling .load
importer.load({
    "connector": connector
    "source": source
    "dest_table": "target",
    "src_table": "source"
})
```

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
    fetch_count=50000
)

# Retrieve list of tables
source.get_tables()

# Retrieve list of columns for a table
source.get_columns("table")

# Retrieve an iterable of all rows for a table
source.retrieve("table")
```

---

## Migrator

The Migrator class handles retrieving data from a Source and importing it into BlazingDB
via an Importer (see above for information on `Sources` and `Importers`)

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

# Create the pipeline to use when processing tables into Blazing
system = pipeline.System([
    pipeline.DropTableStage(),
    pipeline.CreateTableStage()
])

# Create the importer to use when loading data into BlazingDB
importer = importers.ChunkingImporter(
    importers.batchers.RowBatcher(100000),
    "/path/to/blazing/uploads", "user_folder", "data"
)

# Create the migrator using all the pieces above
migrator = blazingdb.Migrator(
    connector, source, system, importer,

    # Set max tables to import concurrently
    import_limit=5
)

# Import all tables into BlazingDB
migrator.migrate()

# Filter the imported tables (supports glob patterns via fnmatch)
migrator.migrate(
    included_tables=["table-one", "table-two", "*-three"],
    excluded_tables=["excluded-three"]
)
```

---

## Pipelines

A pipeline is the main means of configuring the data which is pulled from a `Source`, before it
is imported into BlazingDB. A pipeline is created via the `System` class in the `pipeline` module,
and populated with a series of stages.

```python
from blazingdb import pipeline

system = pipeline.System([
    # Prefix destination tables with "schema"
    pipeline.PrefixTableStage("schema", separator="$"),

    # Recreate existing tables
    pipeline.TruncateTableStage(quiet=False),
    pipeline.DropTableStage(quiet=False),
    pipeline.CreateTableStage(quiet=False),

    # Only import 5000 rows from the source
    pipeline.LimitImportStage(5000),

    # Prompt for user input before continuing
    pipeline.PromptInputStage(prompt="Waiting for input...")
])
```

Pipelines, once created, can then be passed to any `Importer`, or the `Migrator`. An `Importer`
will run its pipeline before and after each request it performs to BlazingDB, whereas the
`Migrator` will run it pipeline before and after each table is imported.

At the moment, the following pipeline stages exist:
 - `CreateTableStage` - Creates target tables before importing data into them
 - `CustomActionStage` - Performs a custom callback before and/or after importing data
 - `CustomCommandStage` - Performs a custom command before and/or after importing data
 - `CustomQueryStage` - Performs a custom SQL query before and/or after importing data
 - `DelayStage` - Pauses the pipeline for the given time (in seconds)
 - `DropTableStage` - Drops existing tables before importing data into them
 - `FilterColumnsStage` - Filters a given set of columns from the imported data
 - `LimitImportStage` - Limits the number of rows imported
 - `PromptInputStage` - Prompts for user input before continuing the pipeline
 - `PostImportHackStage` - Performs a few queries to fix issues BlazingDB has with importing data
 - `PrefixTableStage` - Prefixes the destination table before importing data
 - `TruncateTableStage` - Truncates data a table in BlazingDB (occasionally required for DropTableStage)
