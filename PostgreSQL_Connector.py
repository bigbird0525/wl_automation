'''
Written by Andrew Ravn
Last Updated: 26 JUL 2018
Create connection Object for Postgresql
'''

import psycopg2


class PostgreSQL_Connector(object):

    def __init__(self, hostname, username, pwd, portnum, db):
        super().__init__()
        hostname = hostname
        username = username
        pwd = pwd
        portnum = portnum
        db = db
        try:
            self.conn = psycopg2.connect(dbname=db, host=hostname, port=portnum, user=username, password =pwd)
            self.cursor = self.conn.cursor()
        except:
            print("Unable to connect to DB")

    def query(self, sql_query):
        query = '''{0}'''.format(sql_query)
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.cursor.close()
        return results

