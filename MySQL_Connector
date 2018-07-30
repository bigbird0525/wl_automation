import pymysql
# pip install PyMySQL

class MySQL_Connector(object):

    def __init__(self, hostname, portnum, username, password, db_name):
        super().__init__()
        hostname = hostname
        portnum = portnum
        username = username
        password = password
        dbname = db_name
        self.cursor = pymysql.connect(host=hostname, port=portnum, user=username, passwd=password, db=dbname).cursor(
            pymysql.cursors.DictCursor)

    def query(self, sql_query):

        query = sql_query
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.cursor.close()
        return results
