# coding=utf-8
import requests
import json
import psycopg2 as pg
import threading
import shutil
import sys
from socket import *

class BlazingETL:

    """ Migration Tool """

    def __init__(self, from_connection_obj, to_connection_obj):
        self.from_conn = from_connection_obj
        self.to_conn = to_connection_obj

    def write_chunk_complete(cursor, path, table, file_ext):
        to_open = path+table+file_ext
        print to_open
        file = open(to_open, 'w')
        try:
            for row in cursor.fetchall():
                file.write('|'.join(str(r) for r in row)+'\n')
        except Exception as e:
            print e
        file.close()

    def write_chunk_part(cursor, path, table, file_ext, chunk_size, iterator):
        to_open = path+table+'_'+str(iterator)+file_ext
        print to_open
        file = open(to_open, 'w')
        try:
            for row in cursor.fetchmany(chunk_size):
                #print row
                file.write('|'.join(str(r) for r in row)+'\n')
        except Exception as e:
            print e
        file.close()

    def copy_chunks(from_path, file, to_path):
        print "copy chunks"
        try:
            shutil.copyfile(from_path + file, to_path + file)
        except shutil.Error as e:
            print('Error: %s' % e)
        except IOError as e:
            print('Error: %s' % e.strerror)

    def load_data_stream(cursor, host, port):
        print "load data stream"
        #Create socket
        sSock = socket(AF_INET, SOCK_STREAM)
        #Connect to server
        sSock.connect((host, port))
        try:
            for row in cursor.fetchall():
                print row
                to_send = "load data stream '" + '|'.join(str(r) for r in row)+'\n' + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\n'"
                sSock.send(to_send)
                data = sSock.recv(1024)
                print data
        except Exception as e:
            print e
        sSock.shutdown(0)
        sSock.close()

    def migrate(self, **kwargs):
        """ Supported Migration from Redshift and Postgresql to BlazingDB """

        create_tables = kwargs.get('create_tables', True);
        path = kwargs.get('files_local_path', '/home/second/OneModel/');
        blazing_path = kwargs.get('blazing_files_destination_path', '/opt/blazing/disk1/blazing/blazing-uploads/2/');
        chunk_size = kwargs.get('chunk_size', 100000);
        write_data_chunks = kwargs.get('export_data_from_origin', True);
        copy_data_to_destination = kwargs.get('copy_data_to_blazing', False);
        load_data_into_blazing = kwargs.get('load_data_into_blazing', True);
        file_extension = kwargs.get('file_extension', '.dat');

        query = "select mytables.table_name from INFORMATION_SCHEMA.COLUMNS as i_columns left join information_schema.tables mytables on i_columns.table_name = mytables.table_name where mytables.table_schema = 'public' and mytables.table_type = 'BASE TABLE';"
        cursor = self.from_conn.cursor()
        result = cursor.execute(query)
        status = cursor.statusmessage
        try:
            tables = []
            for row in cursor.fetchall():
                tables.append(row[0]) # tables name
        except:
            print "No results returned"

        # Get table names
        tables_names = set(tables)

        # Loop by tables
        for table in tables_names:

            query = "select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS as i_columns left join information_schema.tables mytables on i_columns.table_name = mytables.table_name where mytables.table_schema = 'public' and mytables.table_type = 'BASE TABLE' and mytables.table_name = '" + table + "';"
            cursor = self.from_conn.cursor()
            result = cursor.execute(query)
            status = cursor.statusmessage

            try:
                columns = []
                for col in cursor.fetchall():

                    # Convert DataTypes and Save String
                    blazing_type = 'datatype'
                    types = {
                        'integer':'long',
                        'character varying':'string('+str(col[2])+')',
                        'character':'string('+str(col[2])+')',
                        'text':'string('+str(col[2])+')',
                        'time with time zone':'string('+str(col[2])+')',
                        'time without time zone':'string('+str(col[2])+')',
                        'timestamp with time zone':'string('+str(col[2])+')',
                        'timestamp without time zone':'string('+str(col[2])+')',
                        '"char"':'string('+str(col[2])+')',
                        'money':'double',
                        'real':'double',
                        'numeric':'double',
                        'double precision':'double',
                        'bigint':'long',
                        'smallint':'long',
                        'bit':'long'
                    }
                    blazing_type = types[col[1]]

                    # Make the describe table line
                    columns.append(col[0] + ' ' + blazing_type)

                # join columns array by table
                columns_desc = ', '.join(columns)

                # Create Tables on Blazing
                if(create_tables==True):
                    result = self.to_conn.run('create table ' + table + ' (' + columns_desc + ')',con)
                    print result.status
                    print result.rows

                # Get data in chunks by table ans save in files
                # Get table content
                query = "select * from "+table
                cursor = self.from_conn.cursor()
                result = cursor.execute(query)
                num_rows = cursor.statusmessage[7:]

                if(int(num_rows) <= int(chunk_size)):

                    """ MultiThread """
                    if(write_data_chunks==True):
                        thread = threading.Thread(target=write_chunk_complete, args=(cursor, path, table, file_extension))
                        thread.start()
                        thread.join()

                    if(copy_data_to_destination==True):
                        thread2 = threading.Thread(target=copy_chunks, args=(path, table + file_extension, blazing_path))
                        thread2.start()
                        thread2.join()

                    # Load Data Infile Blazing
                    if(load_data_into_blazing==True):
                        if(copy_data_to_destination==True):
                            result = self.to_conn.run("load data infile " + table + file_extension + " into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\n'",con)
                        else:
                            result = self.to_conn.run("load data infile '" + path + table + file_extension + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\n'",con)
                        print result.status

                else:
                    # Chunks Division
                    iterations = int(num_rows) / chunk_size

                    for i in range(int(iterations)):

                        """ MultiThread """
                        if(write_data_chunks==True):
                            thread = threading.Thread(target=write_chunk_part, args=(cursor, path, table, file_extension, chunk_size, i))
                            thread.start()
                            thread.join()

                        if(copy_data_to_destination==True):
                            thread2 = threading.Thread(target=copy_chunks, args=(path, table+'_'+str(i)+file_extension, blazing_path))
                            thread2.start()
                            thread2.join()

                        # Load Data Infile Blazing
                        if(load_data_into_blazing==True):
                            if(copy_data_to_destination==True):
                                result = self.to_conn.run("load data infile " + table+"_"+str(i)+file_extension + " into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\n'",con)
                            else:
                                result = self.to_conn.run("load data infile '" + path + table +"_"+str(i)+ file_extension + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\n'",con)
                            print result.status

            # Print Exception
            except Exception as e:
                print e

        # Close ** From DB ** Connection
        self.from_conn.close()

class BlazingResult(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)

    def results_clean(self,j):
        self.__dict__ = json.loads(j)

class BlazingPyConnector:

    def __init__(self, host, username, password, database, **kwargs):
        self.host = host
        self.port = kwargs.get('port', '8089')
        self.username = username
        self.password = password
        self.database = database
        self.protocol = 'http' if (kwargs.get('https', True) == True) else 'http'
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
            print r.content
            result = BlazingResult(r.content)
        else:
            result = BlazingResult('{"status":"fail","rows":"Username or Password incorrect"}')

        return result

