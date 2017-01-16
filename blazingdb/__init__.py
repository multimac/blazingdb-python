# coding=utf-8
import requests
import json
import psycopg2 as pg
import shutil
import sys
from socket import *
import glob, os
import linecache
import logging
import math
import traceback
import tempfile
import threading

class BlazingImporter:
    """ Data Importer """

    def __init__(self, bl_connection):
        self.connection = bl_connection
        self.id_connection = self.connection.connect()

    def small_load_datastream(self, query):
        """small load data stream"""
        print query
        try:
            result = self.connection.run(query, self.id_connection)
            #print result.status
            #print result.rows
        except Exception as e:
            print('Error: %s' % e)

    def get_columns(self, file):
        """ Get Columns and Format Them """
        # Get columns
        columns = linecache.getline(file, 1)
        columns = columns.replace("\n","")
        # Get datatypes
        datatypes = linecache.getline(file, 2)
        datatypes = datatypes.replace("\n","")
        # Columns and Datetypes Arrays
        columns_arr = columns.split("|")
        datatypes_arr = datatypes.split("|")
        # Columns with Datatype Converted
        columns_desc = []
        # Loop by Datatypes
        for i in range(len(datatypes_arr)):
            type = datatypes_arr[i]
            column = columns_arr[i]
            # Convert DataTypes
            blazing_type = 'datatype'
            types = {
                'integer':'long',
                'character varying':'string',
                'character':'string',
                'varchar':'string',
                'text':'string',
                'time with time zone':'string',
                'time without time zone':'string',
                'timestamp with time zone':'string',
                'timestamp without time zone':'string',
                '"char"':'string',
                'money':'double',
                'real':'double',
                'numeric':'double',
                'float':'double',
                'double precision':'double',
                'bigint':'long',
                'smallint':'long',
                'bit':'long'
            }
            type_without_size = [] 
            type_without_size = type.split("(")
            try:
                if(len(type_without_size)>=2):
                    type_without_size[1] = "(" + type_without_size[1]
                    blazing_type = types[type_without_size[0].lower()]
                    blazing_type = blazing_type + type_without_size[1]
                else:
                    blazing_type = types[type.lower()]

            except Exception as e:
                        print "The column datatype cannot be converted to a BlazingDB supported datatype"
            # Make the describe table line
            columns_desc.append(column + ' ' + blazing_type)
        # Join columns array by table
        columns = ', '.join(columns_desc)
        return columns

    def load_data(self, file, table):
        """ Load Data """
        with open(file, "r") as infile:
            data = infile.read()
            lines = data.splitlines()
            for line_number in range(len(lines)):
                if(line_number>1):
                    query = "load data stream '" + lines[line_number] + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\\n'"
                    print query
                    # Load in Thread
                    self.small_load_datastream(query)

    def file_import(self, **kwargs):
        """ File Importer To BlazingDB """
        files_path = kwargs.get('files_path', '/home/second/datasets/')
        columns = kwargs.get('columns', '')
        table = kwargs.get('table', '')
        find_files_in_path = kwargs.get('find_files', True)
        file_ext = kwargs.get('files_extension', '.dat')

        if(find_files_in_path==True):
            # Find Files in Path
            os.chdir(files_path)
            for file in glob.glob("*"+file_ext):
                print(file)
                print(os.path.join(files_path, file))

                # Get Table
                if(table==''):
                    table = file.replace(file_ext,"")
                    print table

                columns = self.get_columns(file)
                print columns

                # Create table
                query = 'create table ' + table + ' (' + columns + ')'
                print query
                try:
                    print "blazing create table"
                    #self.connection.run(query,self.id_connection)
                except Exception as e:
                    print e

                # Load Data into the table
                self.load_data(file, table)

        else:
            # Check if the file exist
            if(os.path.isfile(files_path)):
                print("File not found")
            else:
                print(file)
                print "An only file"
                if(table==''):
                    table = file.replace(file_ext,"")
                    print table

                columns = self.get_columns(file)
                print columns

                # Create table
                query = 'create table ' + table + ' (' + columns + ')'
                print query
                try:
                    print "blazing load"
                    #self.connection.run(query,self.id_connection)
                except Exception as e:
                    print e

                # Load Data into the table
                self.load_data(file, table)

class BlazingQueryException(Exception):
    def __init__(self, result, message=None):
        super(BlazingQueryException, self).__init__(message)

        self.result = result

class BlazingETL(object):
    """ Migration Tool """

    def __init__(self, from_connection, to_connection, **kwargs):
        self.abort = False
        self.dry_run = False

        self.from_conn = from_connection
        self.to_conn = to_connection

        self.join_timeout = kwargs.get('join_timeout', 10)
        self.log_file = kwargs.get('log_file', 'blazing.log')
        self.log_level = getattr(logging, kwargs.get('log_level', 'WARNING').upper())

        self.chunk_size = kwargs.get('chunk_size', 100000)
        self.request_size = kwargs.get('request_size', 1250000)
        self.multithread = kwargs.get('multithread', False)

        self.create_tables = kwargs.get('create_tables', True)
        self.drop_existing = kwargs.get('drop_existing_tables', False)

        self.blazing_env = kwargs.get('blazing_env', None)
        self.blazing_path = kwargs.get('blazing_path', '/blazing-sequential/blazing-uploads/')
        self.local_path = kwargs.get('path', '/home/ubuntu/uploads/')
        self.file_ext = kwargs.get('file_extension', '.dat')

        self.copy_data_to_dest = kwargs.get('copy_data_to_blazing', False)
        self.load_data_into_blazing = kwargs.get('load_data_into_blazing', True)
        self.stream_data_into_blazing = kwargs.get('stream_data_into_blazing', True)
        self.delete_local_after_load = kwargs.get('delete_local_after_load', False)

        self.default_transform = kwargs.get("default_transform", lambda r: r)
        self.field_term = kwargs.get('field_terminator', '|')
        self.field_wrapper = kwargs.get('field_wrapper', '"')
        self.line_term = kwargs.get('line_terminator', '\n')

        self.logger = self.create_logger()

    def create_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(self.log_level)

        console_handler = logging.StreamHandler(sys.stdout)
        file_handler = logging.FileHandler(self.log_file)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    def print_exception(self, message, pause=True):
        exc_info = sys.exc_info()

        if exc_info[0] is None:
            return

        self.logger.exception(message)

        if pause and raw_input("Do you want to continue (y/N)? ").lower() != 'y':
            raise exc_info[1], None, exc_info[2]

    def wrap_field(self, c):
        return self.field_wrapper + c + self.field_wrapper

    def parse_column(self, c, options):
        c = options["trans"](c)
        if c is None:
            return "NULL"

        return str(c) if not isinstance(c, str) else self.wrap_field(c)

    def parse_row(self, row, columns):
        parsed_row = [self.parse_column(r, columns[i]) for i, r in enumerate(row)]

        return self.field_term.join(parsed_row)

    def run_query(self, dest, conn, query, quiet=False):
        self.logger.debug("Performing query of length %d", len(query))

        if self.abort:
            return None

        if self.dry_run:
            self.logger.info(query.encode('unicode_escape'))

            return BlazingResult("""{
                "status": "success",
                "rows": [
                    ["time","0.0","rows","0"],
                    ["message"], ["string"],
                    ["'query dry run\"]
                ]
            }""")

        result = dest.run(query, conn).__dict__

        if not quiet:
            self.logger.info("Response: " + json.dumps(result))

        if result["status"] == "fail":
            self.logger.error("Query failed, see log level INFO for more information")
            raise BlazingQueryException(result)

        return result

    def map_type(self, datatype, size, datetime_size=32):
        types = {
            'bigint': 'long',
            'bit': 'long',
            'boolean': 'long',
            'integer': 'long',
            'smallint': 'long',
            'double precision': 'double',
            'money': 'double',
            'numeric': 'double',
            'real': 'double',
            'character': 'string(' + str(size) + ')',
            'character varying': 'string(' + str(size) + ')',
            'text': 'string(' + str(size) + ')',
            'time with time zone': 'string(' + str(datetime_size) + ')',
            'time without time zone': 'string(' + str(datetime_size) + ')',
            'timestamp with time zone': 'string(' + str(datetime_size) + ')',
            'timestamp without time zone': 'string(' + str(datetime_size) + ')',
            'date': 'date'
        }

        mapped_type = types[datatype]

        if not size:
            self.logger.debug("Mapping type %s to %s", datatype, mapped_type)
        else:
            self.logger.debug("Mapping type %s (size %i) to %s", datatype, size, mapped_type)

        return mapped_type

    def get_filename(self, table, i):
        filename = table + "_" + str(i) + self.file_ext
        if self.blazing_env is not None:
            filename = self.blazing_env + "/" + filename

        return filename

    def write_chunk_part(self, cursor, filename, columns):
        if self.abort:
            return

        if self.dry_run:
            self.logger.info("Writing chunk '%s'", filename)
            return

        chunk_file = open(filename, "w")

        for row in cursor.fetchmany(self.chunk_size):
            chunk_file.write(self.parse_row(row, columns) + self.line_term)

        chunk_file.close()

    def copy_chunks(self, from_path, to_path, file):
        if self.abort:
            return

        if self.dry_run:
            self.logger.info("Copying chunk '%s' from '%s' to '%s'", file, from_path, to_path)
            return

        shutil.copyfile(from_path + file, to_path + file)

    def delete_chunk(self, from_path, file):
        if self.abort:
            return

        if self.dry_run:
            self.logger.info("Deleting chunk '%s' from '%s'", file, from_path)
            return

        os.remove(from_path + file)

    def create_table(self, dest, conn, table, columns):
        col_map = lambda col: col["name"] + " " + col["type"]
        sql_columns = ", ".join(map(col_map, columns))

        self.logger.info("Creating table '%s' with columns '%s'", table, sql_columns)

        query = "create table " + table + " (" + sql_columns + ")"
        self.run_query(dest, conn, query)

    def drop_table(self, dest, conn, table):
        self.logger.info("Dropping table '%s'", table)

        self.run_query(dest, conn, "delete from " + table, True)
        self.run_query(dest, conn, "drop table " + table)

    def load_data(self, dest, conn, table, load_style):
        self.run_query(dest, conn, (
            "load data " + load_style + " into table " + table + " "
            "fields terminated by '" + self.field_term + "' "
            "enclosed by '" + self.field_wrapper + "' "
            "lines terminated by '" + self.line_term + "'"
        ))

    def load_datastream(self, dest, conn, table, batch):
        self.logger.info("Loading data stream of %i rows into table '%s'", len(batch), table)

        try:
            load_style = "stream '" + self.line_term.join(batch) + "'"
            self.load_data(dest, conn, table, load_style)
        except BlazingQueryException:
            self.print_exception("Failed query while loading data stream", pause=False)

    def load_datainfile(self, dest, conn, table, path):
        self.logger.info("Loading data infile '%s' into table '%s'", path, table)

        try:
            self.load_data(dest, conn, table, "infile '" + path + "'")
        except BlazingQueryException:
            self.print_exception("Failed query while loading data infile", pause=False)

    def retrieve_batches(self, cursor, columns):
        batch = []
        batch_size = 0

        chunk = cursor.fetchmany(self.chunk_size)
        while chunk:
            row = self.parse_row(chunk.pop(0), columns)
            if batch_size + len(row) > self.request_size:
                yield batch

                batch = []
                batch_size = 0

            batch.append(row)
            batch_size += len(row)

            if not chunk:
                chunk = cursor.fetchmany(self.chunk_size)

        if batch:
            yield batch

    def migrate_table_stream(self, cursor, dest, conn, table, columns):
        for batch in self.retrieve_batches(cursor, columns):
            self.load_datastream(dest, conn, table, batch)

    def migrate_table_chunk_file(self, dest, conn, table, i):
        filename = self.get_filename(table, i)
        load_path = self.local_path + filename

        if self.copy_data_to_dest:
            self.copy_chunks(self.local_path, self.blazing_path, filename)
            load_path = self.blazing_path + filename

        if self.load_data_into_blazing:
            self.load_datainfile(self.to_conn, conn, table, load_path)

        if self.delete_local_after_load:
            self.delete_chunk(self.local_path, filename)

    def migrate_table_chunks(self, cursor, dest, conn, table, columns):
        iterations = int(math.ceil(float(cursor.rowcount) / self.chunk_size))

        for i in range(iterations):
            filename = self.get_filename(table, i)

            self.write_chunk_part(cursor, self.local_path + filename, columns)
            self.migrate_table_chunk_file(dest, conn, table, i)

    def migrate_table(self, dest, conn, table, options):
        if self.abort:
            return

        self.logger.info("Migrating table '%s'", table)

        schema = options.get('from_schema', 'public')
        type_overrides = options.get('type_overrides', {}).get(table, {})

        cursor = self.from_conn.cursor()
        cursor.execute(
            "select column_name, data_type, character_maximum_length "
            "from information_schema.columns "
            "where table_schema = '" + schema + "' and table_name = '" + table + "'"
        )

        columns = []
        for col in cursor.fetchall():
            override = type_overrides.get(col[0], {})

            mapped_type = override.get("type", self.map_type(col[1], col[2]))
            transform = override.get("trans", self.default_transform)

            columns.append({"name": col[0], "type": mapped_type, "trans": transform})

        # Create Tables on Blazing
        if self.drop_existing:
            self.drop_table(dest, conn, table)

        if self.create_tables:
            self.create_table(dest, conn, table, columns)

        # Get table content
        cursor = self.from_conn.cursor()
        cursor.execute(
            "select " + ", ".join(col["name"] for col in columns) + " " +
            "from " + schema + "." + table
        )

        self.logger.info("%i rows retrieved from source database", cursor.rowcount)

        # Chunks Division
        if self.stream_data_into_blazing:
            self.migrate_table_stream(cursor, dest, conn, table, columns)
        else:
            self.migrate_table_chunks(cursor, dest, conn, table, columns)

    def do_migrate(self, options):
        schema = options.get('from_schema', 'public')
        table_names = options.get('from_tables', None)

        if table_names is None:
            cursor = self.from_conn.cursor()
            cursor.execute(
                "select distinct table_name from information_schema.tables "
                "where table_schema = '" + schema + "' and table_type = 'BASE TABLE'"
            )

            table_names = [row[0] for row in cursor.fetchall()]

        bl_conn = self.to_conn.connect()

        # Unset abort flag
        self.abort = False

        # Loop by tables
        self.logger.info("Tables to migrate: %s", ",".join(table_names))

        threads = []
        for table in table_names:
            if not self.multithread:
                try:
                    self.migrate_table(self.to_conn, bl_conn, table, options)
                except Exception:
                    self.print_exception("Exception caught while migrating a table")
            else:
                migrate_thread = threading.Thread(
                    target=self.migrate_table, args=(self.to_conn, bl_conn, table, options)
                )

                migrate_thread.start()
                threads.append(migrate_thread)

        for thread in threads:
            self.join_with_interrupt(thread)

        # Unset abort flag
        self.abort = False

    def join_with_interrupt(self, thread):
        while thread.isAlive():
            try:
                thread.join(self.join_timeout)
            except KeyboardInterrupt:
                self.abort = True
                self.logger.info("Keyboard interrupt caught, aborting remaining threads...")
            except Exception:
                self.print_exception("Exception caught while waiting on thread to exit", pause=False)

    def migrate(self, **kwargs):
        """ Supported Migration from Redshift and Postgresql to BlazingDB """

        was_dry_run = self.dry_run
        self.dry_run = kwargs.get('dry_run', was_dry_run)

        if "from_tables" in kwargs and isinstance(kwargs["from_tables"], str):
            kwargs["from_tables"] = [kwargs["from_tables"]]

        try:
            self.do_migrate(kwargs)
        except Exception:
            self.print_exception("Exception caught during migration", pause=False)

        self.dry_run = was_dry_run

class BlazingResult(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)

    def results_clean(self, j):
        self.__dict__ = json.loads(j)

class BlazingPyConnector:

    def __init__(self, host, username, password, database, **kwargs):
        self.host = host
        self.port = kwargs.get('port', '8089')
        self.username = username
        self.password = password
        self.database = database
        self.protocol = 'https' if (kwargs.get('https', True) == True) else 'http'
        self.context = kwargs.get('context', '/')
        self.baseurl = self.protocol+'://'+self.host+':'+self.port+self.context
        print "Base URL: " + self.baseurl

    def connect(self):
        connection = False
        try:
            r = requests.post(self.baseurl+'/blazing-jdbc/register', data={'username':self.username, 'password':self.password, 'database':self.database}, verify=False)
            connection = r.content
            if(connection != 'fail'):
                try:
                    r = requests.post(self.baseurl+'/blazing-jdbc/query', data={'username':self.username, 'token':connection, 'query':'use database '+self.database}, verify=False)
                except:
                    print "The database does not exist"
                    raise
            else:
                print "Your username or password is incorrect"
        except:
            print "The host you entered is unreachable or your credentials are incorrect"
            raise

        return connection

    def run(self, query, connection):
        if(connection != False and connection != 'fail'):
            r = requests.post(self.baseurl+'/blazing-jdbc/query', data={'username':self.username, 'token':connection, 'query':query}, verify=False)
            result_key = r.content

            r = requests.post(self.baseurl+'/blazing-jdbc/get-results', data={'username':self.username, 'token':connection, 'resultSetToken':result_key}, verify=False)
            result = BlazingResult(r.content)
        else:
            result = BlazingResult('{"status":"fail","rows":"Username or Password incorrect"}')

        return result
