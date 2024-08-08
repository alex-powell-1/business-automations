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
        r = QueryEngine.query(self.query, commit=self.commit)
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
    SERVER = SERVER
    DATABASE = DATABASE
    USERNAME = USERNAME
    PASSWORD = PASSWORD

    def query(query):
        """Runs Query Against SQL Database. Use Commit Kwarg for updating database"""
        connection = pyodbc.connect(
            f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={QueryEngine.SERVER};PORT=1433;DATABASE={QueryEngine.DATABASE};'
            f'UID={QueryEngine.USERNAME};PWD={QueryEngine.PASSWORD};TrustServerCertificate=yes;timeout=3;ansi=True;',
            autocommit=True,
        )

        connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-16-le')
        connection.setencoding('utf-16-le')

        cursor = connection.cursor()
        query = str(query).strip()
        try:
            response = cursor.execute(query)
            sql_data = response.fetchall()
        except ProgrammingError as e:
            if e.args[0] == 'No results.  Previous SQL was not a query.':
                if cursor.rowcount > 0:
                    sql_data = {'code': 200, 'Affected Rows': cursor.rowcount, 'message': 'success'}
                else:
                    # No rows affected
                    sql_data = {
                        'code': 201,
                        'Affected Rows': cursor.rowcount,
                        'message': 'No rows affected',
                        'query': query,
                    }
            else:
                if len(e.args) > 1:
                    sql_data = {'code': f'{e.args[0]}', 'message': f'{e.args[1]}', 'query': query}
                else:
                    sql_data = {'code': f'{e.args[0]}', 'query': query, 'message': 'Unknown Error'}

        except Error as e:
            if e.args[0] == '40001':
                print('Deadlock Detected. Retrying Query')
                time.sleep(1)
                cursor.execute(query)
            else:
                sql_data = {'code': f'{e.args[0]}', 'message': f'{e.args[1]}', 'query': query}

        cursor.close()
        connection.close()
        return sql_data if sql_data else None
