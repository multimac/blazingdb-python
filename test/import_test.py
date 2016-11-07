from blazingdb import BlazingPyConnector, BlazingImporter

# Blazing Connection
bl = BlazingPyConnector('localhost','test@blazingdb.com','tester','test')
print "Connection"

importer = BlazingImporter(bl)
importer.file_import()