from blazingdb import BlazingPyConnector, BlazingETL

bl = BlazingPyConnector('localhost','test@blazingdb.com','test','db_name')
con = bl.connect()
result = bl.run("create table my_new_table (field1 string(20), field2 long, field3 date, field4 double)",con)
print result.status
print result.rows
