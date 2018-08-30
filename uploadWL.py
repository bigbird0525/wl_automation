import bidder_optimizations.MySQL_Connector as mSQL

class uploadWL(object):

    def __init__(self):
        super().__init__()

    def upload(self, newWL):
        '''
        Deletes old WL and uploads new WL
        :param data:
        :return:
        '''

        line_item_id = newWL[0]['line_item_id']
        subid = ""
        for rows in newWL:
            subid += str(rows['subid']+",")

        query = '''
            {0}
        '''.format(line_item_id, subid) # Input Update query to make changes to database

        mSQL.MySQL_Connector("{0}").query(query) # Input necessary connection details such as mySQL address, username, port, db, password
