import psycopg2 as pg2
import pandas as pd

class DataBaseConnection:
    def __init__(self, user, password, dbname, host='localhost', port=5432):
        self._user = user
        self._password = password
        self._dbname = dbname
        self._host = host
        self._port = port

    def run_query(self, query):
        connection = pg2.connect(
            host = self._host,
            port = self._port,
            dbname = self._dbname,
            user = self._user,
            password = self._password
        )
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        data = cursor.fetchall()
        cursor.close()
        connection.close()
        colnames = [desc.name for desc in cursor.description]
        return pd.DataFrame(data, columns=colnames)