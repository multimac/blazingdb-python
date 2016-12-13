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

    def write_chunk_part(self, cursor, chunk_size, filename):
        chunk_file = open(filename, "w")
        for row in cursor.fetchmany(chunk_size):
            chunk_file.write(self.parse_row(row) + '\n')

        chunk_file.close()

    def copy_chunks(self, from_path, to_path, file):
        shutil.copyfile(from_path + file, to_path + file)

    def load_datastream(self, cursor, table, dest, conn, request_size):
        batch = []
        batch_size = 0

        cursor_row = cursor.fetchone()
        while cursor_row is not None:
            row = self.parse_row(cursor.fetchone())

            batch.append(row)
            batch_size += len(row)

            if batch_size > request_size:
                query = (
                    "load data stream '" + '\n'.join(batch) + "' "
                    "into table " + table + " fields terminated by '|' "
                    "enclosed by '\"' lines terminated by '\n'"
                )

                dest.run(query, conn)

                batch = []
                batch_size = 0

        if len(batch) != 0:
            query = (
                "load data stream '" + '\n'.join(batch) + "' "
                "into table " + table + " fields terminated by '|' "
                "enclosed by '\"' lines terminated by '\n'"
            )

            dest.run(query, conn)


    def migrate_table(self, table, bl_conn, options):
        create_tables = options.get('create_tables', True)
        schema = options.get('schema', 'public')

        chunk_size = options.get('chunk_size', 100000)
        request_size = options.get('request_size', 1250000)

        blazing_env = options.get('blazing_env', None)
        blazing_path = options.get('blazing_path', '/blazing-sequential/blazing-uploads/')
        local_path = options.get('local_path', '/home/ubuntu/uploads/')
        file_ext = options.get('file_ext', '.dat')

        copy_data_to_destination = options.get('copy_data_to_blazing', False)
        load_data_into_blazing = options.get('load_data_into_blazing', True)
        stream_data_into_blazing = options.get('stream_data_into_blazing', True)

        types = lambda datatype, size: {
            'bigint': 'long',
            'bit': 'long',
            'boolean': 'long',
            'integer': 'long',
            'smallint': 'long',
            'double precision': 'double',
            'money': 'double',
            'numeric': 'double',
            'real': 'double',
            'character varying': 'string(' + str(size) + ')',
            'character': 'string(' + str(size) + ')',
            'text': 'string(' + str(size) + ')',
            'time with time zone': 'string(32)',
            'time without time zone': 'string(32)',
            'timestamp with time zone': 'string(32)',
            'timestamp without time zone': 'string(32)',
            'date': 'date'
        }[datatype]

        cursor = self.from_conn.cursor()
        cursor.execute(
            "select column_name, data_type, character_maximum_length "
            "from information_schema.columns "
            "where table_schema = '" + schema + "' and table_name = '" + table + "'"
        )

        columns = [{"name": col[0], "type": types(col[1], col[2])} for col in cursor.fetchall()]

        # Create Tables on Blazing
        if create_tables:
            col_map = lambda col: col["name"] + " " + col["type"]
            query = "create table " + table + " (" + ", ".join(map(col_map, columns)) + ")"

            self.to_conn.run(query, bl_conn)

        # Get table content
        cursor = self.from_conn.cursor()
        cursor.execute(
            "select " + ", ".join(col["name"] for col in columns) + " " +
            "from " + schema + "." + table
        )

        num_rows = cursor.rowcount

        # Chunks Division
        if stream_data_into_blazing:
            self.load_datastream(cursor, table, self.to_conn, bl_conn, request_size)
        else:
            iterations = int(math.ceil(num_rows / chunk_size))
            for i in range(iterations):
                filename = table + "_" + str(i) + file_ext
                if blazing_env is not None:
                    filename = blazing_env + "/" + filename

                self.write_chunk_part(cursor, chunk_size, local_path + filename)

                path = local_path + filename
                if copy_data_to_destination:
                    self.copy_chunks(local_path, blazing_path, filename)
                    path = blazing_path + filename

                if load_data_into_blazing:
                    query = (
                        "load data infile " + path + " into table " + table + " "
                        "fields terminated by '|' enclosed by '\"' lines terminated by '\n'"
                    )

                    self.to_conn.run(query, bl_conn)

    def do_migrate(self, options):
        schema = options.get('schema', 'public')

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
                self.migrate_table(table, bl_conn, options)
            except Exception:
                self.print_exception()


    def migrate(self, **kwargs):
        """ Supported Migration from Redshift and Postgresql to BlazingDB """

        try:
            self.do_migrate(kwargs)
        except Exception:
            self.print_exception(False)

        # Close ** From DB ** Connection
        self.from_conn.close()

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
        print "Running query of length: " + str(len(query))
        print "With first line: " + query.split('\n', 1)[0]

        if(connection != False and connection != 'fail'):
            r = requests.post(self.baseurl+'/blazing-jdbc/query', data={'username':self.username, 'token':connection, 'query':query}, verify=False)
            result_key = r.content
            r = requests.post(self.baseurl+'/blazing-jdbc/get-results', data={'username':self.username, 'token':connection, 'resultSetToken':result_key}, verify=False)
            print r.content
            result = BlazingResult(r.content)
        else:
            result = BlazingResult('{"status":"fail","rows":"Username or Password incorrect"}')

        return result
