import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine, engine


class ConnectDB:

    def __init__(self):
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, db='krx_stock_data',
                                    user='root', passwd='abcd123456', autocommit=True)
        self.cursor = self.conn.cursor()

    def executeQuery(self, query):
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall())
        df.columns = [col[0] for col in self.cursor.description]
        return df

    def closeConnection(self):
        self.cursor.close()
        self.conn.close()


class EngineDB:

    def __init__(self):
        self.server = '127.0.0.1'
        self.user = 'root'
        self.passwd = 'abcd123456'
        self.db = 'krx_stock_data'

    def getEngine(self):
        # sqlalchemy의 create_engine을 이용하여 DB 연결
        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(
            self.user, self.passwd, self.server, self.db))
        return engine
