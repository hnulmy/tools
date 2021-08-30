#!/usr/bin/python
# -*- coding: UTF-8 -*-

import sys
import copy
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from event_pb2 import EventList

LOCALHOST = ''
PORT = 0
USER = ''
PASSWORD = ''
DATABASE = ''

class ReadDB():
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(USER, PASSWORD, LOCALHOST, PORT, DATABASE))
        #pd.set_option('display.max_columns', None)
    def unmarshal(self, pb_message):
        message = EventList()
        message.ParseFromString(pb_message)
        return message
    def run(self, user_id):
        sql_query = 'select * from table where user_id = {}'.format(user_id)
        df_marshal = pd.read_sql_query(sql_query, self.engine)
        df_unmarshal = copy.deepcopy(df_marshal)
        #print(df_marshal)
        for index, row in df_marshal.iterrows():
            df_unmarshal.at[index, 'content'] = self.unmarshal(row['content'])
        filename = user_id + '.csv'
        df_unmarshal.to_csv(filename)

def main(user_id):
    readDB= ReadDB()
    readDB.run(user_id)

if __name__ == '__main__':
    user_id = sys.argv[1]
    main(user_id)