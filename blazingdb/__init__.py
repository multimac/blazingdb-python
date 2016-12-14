# coding=utf-8
import requests
import json
import psycopg2 as pg
import shutil
import sys
from socket import *
import glob, os
import linecache
import math
import traceback
import tempfile

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

class BlazingETL(object):
    """ Migration Tool """

    def __init__(self, from_connection_obj, to_connection_obj):
        self.from_conn = from_connection_obj
        self.to_conn = to_connection_obj

    def print_exception(self, pause=True):
        exc_info = sys.exc_info()

        if exc_info[0] is None:
            return

        print traceback.format_exc()

        if pause and raw_input("Do you want to continue (y/N)? ").lower() != 'y':
            raise exc_info[1], None, exc_info[2]

    def parse_row(self, row):
        return '|'.join(str(r if r is not None else 'NULL') for r in row)

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

        return types[datatype]

    def write_chunk_part(self, cursor, chunk_size, filename):
        chunk_file = open(filename, "w")
        for row in cursor.fetchmany(chunk_size):
            chunk_file.write(self.parse_row(row) + '\n')

        chunk_file.close()

    def copy_chunks(self, from_path, to_path, file):
        shutil.copyfile(from_path + file, to_path + file)

    def delete_chunk(self, from_path, file):
        os.remove(from_path + file)

    def create_table(self, dest, conn, table, columns):
        col_map = lambda col: col["name"] + " " + col["type"]
        sql_columns = ", ".join(map(col_map, columns))

        print "Creating table '" + table + "' with columns "+ sql_columns

        query = "create table " + table + " (" + sql_columns + ")"

        dest.run(query, conn)

    def load_data(self, dest, conn, table, load_style, options):
        field_term = options.get('field_terminator', '|')
        field_wrapper = options.get('field_wrapper', '"')
        line_term = options.get('line_terminator', '\n')

        result = dest.run((
            "load data " + load_style + " into table " + table + " "
            "fields terminated by '" + field_term + "' enclosed by '" + field_wrapper + "' "
            "lines terminated by '" + line_term + "'"
        ), conn)

        print "Data load into '" + table + "' returned: " + json.dumps(result)

    def load_datastream(self, dest, conn, table, batch, options):
        print "Loading data stream of " + len(batch) + " rows into table '" + table + "'"

        line_term = options.get('line_terminator', '\n')
        load_style = "stream '" + line_term.join(batch) + "'"

        self.load_data(dest, conn, table, load_style, options)

    def load_datainline(self, dest, conn, table, path, options):
        print "Loading data inline '" + path + "' into table '" + table + "'"
        self.load_data(dest, conn, table, "infile " + path, options)

    def migrate_table_stream(self, cursor, table, dest, conn, options):
        chunk_size = options.get('chunk_size', 100000)
        request_size = options.get('request_size', 1250000)

        chunk = cursor.fetchmany(chunk_size)

        while chunk:
            batch = []
            batch_size = 0

            # Populate a batch of rows to send to Blazing
            while chunk and batch_size < request_size:
                row = self.parse_row(chunk.pop(0))

                batch.append(row)
                batch_size += len(row)

                if not chunk:
                    chunk = cursor.fetchmany(chunk_size)

            self.load_datastream(dest, conn, table, batch, options)

    def migrate_table_chunks(self, cursor, table, dest, conn, options):
        chunk_size = options.get('chunk_size', 100000)

        blazing_env = options.get('blazing_env', None)
        blazing_path = options.get('blazing_path', '/blazing-sequential/blazing-uploads/')

        local_path = options.get('path', '/home/ubuntu/uploads/')
        file_ext = options.get('file_extension', '.dat')

        copy_data_to_dest = options.get('copy_data_to_blazing', False)
        load_data_into_blazing = options.get('load_data_into_blazing', True)
        delete_local_after_load = options.get('delete_local_after_load', False)

        iterations = int(math.ceil(cursor.rowcount / chunk_size))

        for i in range(iterations):
            filename = table + "_" + str(i) + file_ext
            if blazing_env is not None:
                filename = blazing_env + "/" + filename

            self.write_chunk_part(cursor, chunk_size, local_path + filename)

            load_path = local_path + filename
            if copy_data_to_dest:
                self.copy_chunks(local_path, blazing_path, filename)
                load_path = blazing_path + filename

            if load_data_into_blazing:
                self.load_datainline(self.to_conn, conn, table, load_path, options)

            if delete_local_after_load:
                self.delete_chunk(local_path, filename)


    def migrate_table(self, dest, conn, table, options):
        print "Migrating table '" + table + "'"

        create_tables = options.get('create_tables', True)
        schema = options.get('from_schema', 'public')

        stream_data_into_blazing = options.get('stream_data_into_blazing', True)

        cursor = self.from_conn.cursor()
        cursor.execute(
            "select column_name, data_type, character_maximum_length "
            "from information_schema.columns "
            "where table_schema = '" + schema + "' and table_name = '" + table + "'"
        )

        columns = [
            {"name": col[0], "type": self.map_type(col[1], col[2])} for col in cursor.fetchall()
        ]

        # Create Tables on Blazing
        if create_tables:
            self.create_table(dest, conn, table, columns)

        # Get table content
        cursor = self.from_conn.cursor()
        cursor.execute(
            "select " + ", ".join(col["name"] for col in columns) + " " +
            "from " + schema + "." + table
        )

        # Chunks Division
        if stream_data_into_blazing:
            self.migrate_table_stream(cursor, table, dest, conn, options)
        else:
            self.migrate_table_chunks(cursor, table, dest, conn, options)

    def do_migrate(self, options):
        schema = options.get('from_schema', 'public')

        cursor = self.from_conn.cursor()
        cursor.execute(
            "select distinct table_name from information_schema.tables "
            "where table_schema = '" + schema + "' and table_type = 'BASE TABLE'"
        )

        tables_names = [row[0] for row in cursor.fetchall()]

        # Loop by tables
        bl_conn = self.to_conn.connect()
        for table in tables_names:
            try:
                self.migrate_table(self.to_conn, bl_conn, table, options)
            except Exception:
                self.print_exception()


    def migrate(self, **kwargs):
        """ Supported Migration from Redshift and Postgresql to BlazingDB """

        try:
            self.do_migrate(kwargs)
        except Exception:
            self.print_exception(False)

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
