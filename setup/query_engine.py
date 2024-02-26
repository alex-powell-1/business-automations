from setup.creds import *
import pyodbc


class QueryEngine:
    def __init__(self):
        self.__SERVER = SERVER
        self.__DATABASE = DATABASE
        self.__USERNAME = USERNAME
        self.__PASSWORD = PASSWORD

    def query_db(self, query, commit=False):
        """Runs Query Against SQL Database. Use Commit Kwarg for updating database"""
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.__SERVER};PORT=1433;DATABASE={self.__DATABASE};'
            f'UID={self.__USERNAME};PWD={self.__PASSWORD};TrustServerCertificate=yes;timeout=3')
        cursor = connection.cursor()
        if commit:
            sql_data = cursor.execute(query)
            connection.commit()
        else:
            sql_data = cursor.execute(query).fetchall()
        cursor.close()
        connection.close()
        if sql_data:
            return sql_data
        else:
            return
