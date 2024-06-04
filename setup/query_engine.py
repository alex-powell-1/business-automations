from setup.creds import *
import pyodbc
from pyodbc import ProgrammingError


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
        connection.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
        connection.setencoding('latin1')

        cursor = connection.cursor()
        if commit:
            try:
                cursor.execute(query)
                connection.commit()
            except ProgrammingError as e:
                sql_data = {"code": f"{e.args[0]}", "message": f"{e.args[1]}"}
            else:
                sql_data = {"code": 200, "message": "Query Successful"}
        else:
            try:
                sql_data = cursor.execute(query).fetchall()
            except ProgrammingError as e:
                sql_data = {"code": f"{e.args[0]}", "message": f"{e.args[1]}"}

        cursor.close()
        connection.close()
        return sql_data if sql_data else None
