from setup.creds import SERVER, DATABASE, USERNAME, PASSWORD
import pyodbc
from pyodbc import ProgrammingError, Error
import time

from typing import Callable


# WIP
class Query:
    def __init__(self, query, cb: Callable, commit=False):
        self.query = query
        self.commit = commit
        self.cb = cb

    def run_query(self):
        qe = QueryEngine()
        r = qe.query_db(self.query, commit=self.commit)
        if self.cb:
            self.cb(r)


# WIP
class QueryQueue:
    def __init__(self):
        self.queue: list[Query] = []
        self.running = False

    def run_queue(self):
        self.running = True
        for i, query in enumerate(self.queue):
            query.run_query()
            self.queue.pop(i)

        self.running = False

    def add_query(self, query, cb: Callable, commit=False):
        self.queue.append(Query(query=query, cb=cb, commit=commit))
        if not self.running:
            self.run_queue()

    def query_db(self, query, commit=False):
        def cb(r):
            return r

        self.add_query(query=query, cb=cb, commit=commit)


class QueryEngine:
    def __init__(self):
        self.__SERVER = SERVER
        self.__DATABASE = DATABASE
        self.__USERNAME = USERNAME
        self.__PASSWORD = PASSWORD

    def query_db(self, query, commit=False):
        """Runs Query Against SQL Database. Use Commit Kwarg for updating database"""
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.__SERVER};PORT=1433;DATABASE={self.__DATABASE};"
            f"UID={self.__USERNAME};PWD={self.__PASSWORD};TrustServerCertificate=yes;timeout=3"
        )
        connection.setdecoding(pyodbc.SQL_CHAR, encoding="latin1")
        connection.setencoding("latin1")

        cursor = connection.cursor()
        if commit:
            try:
                cursor.execute(query)
                connection.commit()
            except ProgrammingError as e:
                sql_data = {"code": f"{e.args[0]}", "message": f"{e.args[1]}"}
            except Error as e:
                if e.args[0] == "40001":
                    print("Deadlock Detected. Retrying Query")
                    time.sleep(1)
                    cursor.execute(query)
                    connection.commit()
                else:
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
