# coding=utf-8
import requests
import json

class BlazingResult(object):
    def __init__(self, j):
        self.__dict__ = json.loads(j)
    
    def results_clean(self,j):
        self.__dict__ = json.loads(j)
           
class BlazingPyConnector:

    def __init__(self, host, username, password, database, **kwargs):
        self.host = host
        self.port = kwargs.get('port', '8443')
        self.username = username
        self.password = password
        self.database = database
        
    def connect(self):
        connection = False
        try:
            r = requests.post('https://'+self.host+':'+self.port+'/blazing-jdbc/register', data={'username':self.username, 'password':self.password, 'database':self.database}, verify=False)
            connection = r.content
            if(connection != 'fail'):
                try:
                    r = requests.post('https://'+self.host+':'+self.port+'/blazing-jdbc/query', data={'username':self.username, 'token':connection, 'query':'use database '+self.database}, verify=False)
                except:
                    print "The database does not exist"
            else:
                print "Your username or password is incorrect"
        except:
            print "The host you entered is unreachable or your credentials are incorrect"

        return connection
        
    def run(self, query, connection):
        if(connection != False and connection != 'fail'):
            r = requests.post('https://'+self.host+':'+self.port+'/blazing-jdbc/query', data={'username':self.username, 'token':connection, 'query':query}, verify=False)
            result_key = r.content
            r = requests.post('https://'+self.host+':'+self.port+'/blazing-jdbc/get-results', data={'username':self.username, 'token':connection, 'resultSetToken':result_key}, verify=False)
            result = BlazingResult(r.content)
        else:
            result = BlazingResult('{"status":"fail","rows":"Username or Password incorrect"}')

        return result
        