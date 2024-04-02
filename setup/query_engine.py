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

# db = QueryEngine()
#
# response = db.query_db("SELECT * FROM AR_CUST")
# for x in response[0:5]:
#     print(x)


# def add_size_to_single_products():
#     """Use regular expressions to take number out of long description and make size field"""
#     import re
#     query = """
#     SELECT ITEM_NO, LONG_DESCR
#     FROM IM_ITEM
#     WHERE CATEG_COD != 'SUPPLIES' AND LONG_DESCR like '%[0-9]G' AND USR_PROF_ALPHA_17 IS NOT NULL
#     """
#     db = QueryEngine()
#     response = db.query_db(query)
#     if response is not None:
#         counter = 1
#         for x in response[212:]:
#             print(f"#{counter}/{len(response)}: {x[0]}, {x[1]}")
#             description = x[1]
#             number = re.findall(r'\d+', description)
#             new_name = f"{number[0]} Gallon"
#             query = f"""
#             UPDATE IM_ITEM
#             SET USR_PROF_ALPHA_15 = NULL, LST_MAINT_DT = GETDATE()
#             WHERE ITEM_NO = '{x[0]}'
#             """
#             db.query_db(query, commit=True)
#             counter += 1


