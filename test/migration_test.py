from blazingdb import BlazingPyConnector, BlazingETL
import psycopg2 as postgres

# Blazing Connection
bl = BlazingPyConnector('localhost','test@blazingdb.com','tester','nada')
print "Connection"
# Postgresql Connection
pg = postgres.connect(host='localhost',dbname='nada',user='postgres',password='postgres')
print "Dos"
migrator = BlazingETL(pg, bl)
print "Tres"
migrator.migrate(
    create_tables=False,                                            # Specify if it's needed to create the tables in the migration
    files_path='/home/user/OneModel/',                              # Specify where will the migration data files stored
    blazing_path='/opt/blazing/disk1/blazing/blazing-uploads/',     # Specify in case it's wanted to copy the data to BlazingDB uploads folder
    chunk_size=100000,                                              # Specify the size of the chunks of data to migrate
    export_data_from_origin=False,                                  # Turn to False in case you already have the and only need to load them into BlazingDb
    copy_data_to_blazing=False,                                     # Turn to True in case you want to copy the data to BlazingDB uploads folder
    load_data_into_blazing=True,                                    # Turn to False in case you want to migrate only the structure and not the data
    file_extension='.dat',                                          # Specify the data files extension
    by_stream=False                                                 # Change to false if you need the migration files generated
)
