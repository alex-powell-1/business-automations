import random
import math
from setup import creds
from shortuuid import ShortUUID
from setup.creds import Table
from setup.utilities import PhoneNumber, EmailAddress
from setup.error_handler import ProcessOutErrorHandler, ProcessInErrorHandler, LeadFormErrorHandler
from datetime import datetime, timedelta
from traceback import format_exc as tb
from time import sleep
import re
import pyodbc


class Database:
    SERVER = creds.SQL.SERVER
    DATABASE = creds.SQL.DATABASE
    PORT = creds.SQL.PORT
    USERNAME = creds.SQL.USERNAME
    PASSWORD = creds.SQL.PASSWORD
    error_handler = ProcessOutErrorHandler.error_handler
    logger = ProcessOutErrorHandler.logger

    def query(query, mapped=False):
        """Runs Query Against SQL Database. Use Commit Kwarg for updating database"""

        connection_string = f"""
        DRIVER={{ODBC Driver 18 for SQL Server}};
        SERVER={Database.SERVER};
        PORT={Database.PORT};
        DATABASE={Database.DATABASE};
        UID={Database.USERNAME};
        PWD={Database.PASSWORD};
        TrustServerCertificate=yes;
        timeout=3;
        ansi=True;
        """

        connection = pyodbc.connect(connection_string, autocommit=True)

        connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-16-le')
        connection.setencoding('utf-16-le')

        cursor = connection.cursor()
        query = str(query).strip()
        try:
            response = cursor.execute(query)
            sql_data = response.fetchall()
        except pyodbc.ProgrammingError as e:
            if e.args[0] == 'No results.  Previous SQL was not a query.':
                if cursor.rowcount > 0:
                    sql_data = {'code': 200, 'affected rows': cursor.rowcount, 'message': 'success'}
                else:
                    # No rows affected
                    sql_data = {
                        'code': 201,
                        'affected rows': cursor.rowcount,
                        'message': 'No rows affected',
                        'query': query,
                    }
            else:
                if len(e.args) > 1:
                    sql_data = {'code': f'{e.args[0]}', 'message': f'{e.args[1]}', 'query': query}
                else:
                    sql_data = {'code': f'{e.args[0]}', 'query': query, 'message': 'Unknown Error'}

        except pyodbc.Error as e:
            if e.args[0] == '40001':
                Database.logger.warn('Deadlock Detected. Retrying Query')
                sleep(1)
                Database.query(query)
            else:
                sql_data = {'code': f'{e.args[0]}', 'message': f'{e.args[1]}', 'query': query}

        except KeyboardInterrupt:
            Database.logger.warn('Keyboard Interrupt. Query Cancelled.')
            raise KeyboardInterrupt

        except Exception as e:
            Database.error_handler.add_error_v(error=e, origin='database.py', traceback=tb())
            raise Exception(e)
        else:
            if mapped:
                column_response = cursor.description
                if sql_data:
                    code = 200
                    message = 'success'
                    mapped_response: list[dict] = []  # list of dictionaries
                    row_count = len(sql_data)
                    for row in sql_data:
                        row_dict = {}
                        for i, column in enumerate(row):
                            column_name = column_response[i][0]
                            row_dict[column_name] = column
                        mapped_response.append(row_dict)
                else:
                    code = 201
                    message = 'No results found'
                    row_count = 0
                    mapped_response = [{column[0]: None for column in column_response}]

                sql_data = {'code': code, 'message': message, 'rows': row_count, 'data': mapped_response}
        finally:
            cursor.close()
            connection.close()
            return sql_data if sql_data else None

    def sql_scrub(string):
        """Sanitize a string for use in SQL queries."""
        escapes = ''.join([chr(char) for char in range(1, 32)])
        return string.strip().replace("'", "''").translate(str.maketrans('', '', escapes))

    def create_tables():
        tables = {
            'design_leads': f"""
                                        CREATE TABLE {Table.design_leads} (
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        CUST_NO varchar(50),
                                        FST_NAM varchar(50),
                                        LST_NAM varchar(50),
                                        EMAIL varchar(60), 
                                        PHONE varchar(40),
                                        SKETCH bit DEFAULT(0),
                                        SCALED bit DEFAULT(0),
                                        DIGITAL bit DEFAULT(0),
                                        ON_SITE bit DEFAULT(0), 
                                        DELIVERY bit DEFAULT(0),
                                        INSTALL bit DEFAULT(0),
                                        TIMELINE varchar(50), 
                                        STREET varchar(75),
                                        CITY varchar(50),
                                        STATE varchar(20),
                                        ZIP varchar(10),
                                        COMMENTS varchar(500)
                                        )""",
            'qr': f"""
                                        CREATE TABLE {Table.qr} (
                                        QR_CODE varchar(100) NOT NULL PRIMARY KEY,
                                        URL varchar(100),
                                        PUBLICATION varchar(50),
                                        MEDIUM varchar(50),
                                        OFFER varchar(100),
                                        DESCR varchar(255),
                                        COUPON_CODE varchar(20),
                                        COUPON_USE int DEFAULT(0),
                                        VISIT_COUNT int DEFAULT(0),
                                        CREATE_DT datetime NOT NULL DEFAULT(current_timestamp),
                                        LST_SCAN datetime
                                        )""",
            'qr_activ': f"""
                                        CREATE TABLE {Table.qr_activity} (
                                        SCAN_DT datetime NOT NULL DEFAULT(current_timestamp) PRIMARY KEY,
                                        CODE varchar(100) NOT NULL FOREIGN KEY REFERENCES SN_QR(QR_CODE),
                                        );""",
            'sms': f"""
                                        CREATE TABLE {Table.sms}(
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        ORIGIN varchar(30),
                                        CAMPAIGN varchar(30),
                                        DIRECTION varchar(15),
                                        TO_PHONE varchar(30),
                                        FROM_PHONE varchar(30),
                                        BODY varchar(500), 
                                        USERNAME varchar(50),
                                        CUST_NO varchar(50),
                                        NAME varchar(80),
                                        CATEGORY varchar(50),
                                        MEDIA varchar(500), 
                                        SID varchar(100),
                                        ERROR_CODE varchar(20),
                                        ERROR_MESSAGE varchar(255)
                                        )""",
            'sms_event': f"""
                                        CREATE TABLE {Table.sms_event}(
                                        ID int IDENTITY(1,1) PRIMARY KEY,
                                        DATE datetime NOT NULL DEFAULT(current_timestamp),
                                        ORIGIN varchar(30),
                                        CAMPAIGN varchar(30),
                                        PHONE varchar(30),
                                        CUST_NO varchar(50),
                                        NAME varchar(80),
                                        CATEGORY varchar(50),
                                        EVENT_TYPE varchar(20),
                                        MESSAGE varchar(255)
                                        )""",
            'sms_subsribe': f"""
                                        CREATE TABLE {Table.sms_subsribe}(
                                        CUST_NO varchar(15) NOT NULL,
                                        PHONE_1 T_BOOL DEFAULT('N'),
                                        PHONE_1_FST_SUB_DT DATETIME,
                                        PHONE_1_MAINT_DT DATETIME,
                                        PHONE_2 T_BOOL DEFAULT('N'),
                                        PHONE_2_FST_SUB_DT DATETIME,
                                        PHONE_2_MAINT_DT DATETIME,
                                        LST_MAINT_DT DATETIME DEFAULT(current_timestamp))""",
            'stock_notify': f"""
                                        CREATE TABLE {Table.stock_notify}(
                                        ID int IDENTITY(1,1) primary key,
                                        ITEM_NO varchar(16) NOT NULL,
                                        EMAIL varchar(64),
                                        PHONE varchar(16),
                                        CREATED_DT DATETIME DEFAULT(current_timestamp)
                                        );""",
            'newsletter': f"""
                                        CREATE TABLE {Table.newsletter} (
                                        ID int identity(1,1) primary key,
                                        EMAIL varchar(50),
                                        CREATED_DT DATETIME DEFAULT(current_timestamp)
                                        );""",
            'email_list': f"""
                                        CREATE TABLE {Table.email_list} (
                                        CUST_NO varchar(15) NOT NULL,
                                        LIST_1 T_BOOL DEFAULT('N'),
                                        LIST_1_FST_SUB_DT DATETIME,
                                        LIST_1_MAINT_DT DATETIME,

                                        LIST_2 T_BOOL DEFAULT('N'),
                                        LIST_2_FST_SUB_DT DATETIME,
                                        LIST_2_MAINT_DT DATETIME,

                                        LIST_3 T_BOOL DEFAULT('N'),
                                        LIST_3_FST_SUB_DT DATETIME,
                                        LIST_3_MAINT_DT DATETIME,

                                        LIST_4 T_BOOL DEFAULT('N'),
                                        LIST_4_FST_SUB_DT DATETIME,
                                        LIST_4_MAINT_DT DATETIME,

                                        LIST_5 T_BOOL DEFAULT('N'),
                                        LIST_5_FST_SUB_DT DATETIME,
                                        LIST_5_MAINT_DT DATETIME,

                                        LIST_6 T_BOOL DEFAULT('N'),
                                        LIST_6_FST_SUB_DT DATETIME,
                                        LIST_6_MAINT_DT DATETIME,

                                        LIST_7 T_BOOL DEFAULT('N'),
                                        LIST_7_FST_SUB_DT DATETIME,
                                        LIST_7_MAINT_DT DATETIME,

                                        LIST_8 T_BOOL DEFAULT('N'),
                                        LIST_8_FST_SUB_DT DATETIME,
                                        LIST_8_MAINT_DT DATETIME,

                                        LIST_9 T_BOOL DEFAULT('N'),
                                        LIST_9_FST_SUB_DT DATETIME,
                                        LIST_9_MAINT_DT DATETIME,

                                        LIST_10 T_BOOL DEFAULT('N'),
                                        LIST_10_FST_SUB_DT DATETIME,
                                        LIST_10_MAINT_DT DATETIME,
                                        LST_MAINT_DATE DATETIME DEFAULT(current_timestamp));""",
        }

        for table in tables:
            Database.query(tables[table])

    class DesignLead:
        def get(yesterday=True):
            if yesterday:
                query = f"""
                SELECT * FROM {Table.design_leads}
                WHERE DATE > '{datetime.now().date() - timedelta(days=1)}' AND DATE < '{datetime.now().date()}'
                """
            else:
                query = f"""
                    SELECT * FROM {Table.design_leads}
                    """
            return Database.query(query)

        def insert(
            date,
            cust_no,
            first_name,
            last_name,
            email,
            phone,
            interested_in,
            timeline,
            street,
            city,
            state,
            zip_code,
            comments,
            eh=LeadFormErrorHandler,
        ):
            sketch, scaled, digital, on_site, delivery, install = 0, 0, 0, 0, 0, 0
            if interested_in is not None:
                for x in interested_in:
                    if x == 'FREE Sketch-N-Go Service':
                        sketch = 1
                    if x == 'Scaled Drawing':
                        scaled = 1
                    if x == 'Digital Renderings':
                        digital = 1
                    if x == 'On-Site Consultation':
                        on_site = 1
                    if x == 'Delivery & Placement Service':
                        delivery = 1
                    if x == 'Professional Installation':
                        install = 1

            first_name = Database.sql_scrub(first_name)
            last_name = Database.sql_scrub(last_name)
            street = Database.sql_scrub(street)
            city = Database.sql_scrub(city)
            state = Database.sql_scrub(state)
            comments = Database.sql_scrub(comments)

            query = f"""
                INSERT INTO {Table.design_leads} (DATE, CUST_NO, FST_NAM, LST_NAM, EMAIL, PHONE, SKETCH, SCALED, DIGITAL, 
                ON_SITE, DELIVERY, INSTALL, TIMELINE, STREET, CITY, STATE, ZIP, COMMENTS)
                VALUES ('{date}', {f"'{cust_no}'" if cust_no else 'NULL'}, '{first_name}', '{last_name}', '{email}', '{phone}', {sketch}, 
                {scaled}, {digital}, {on_site}, {delivery}, {install}, '{timeline}', 
                '{street}', '{city}', '{state}', '{zip_code}', '{comments}')
                """
            response = Database.query(query)
            if response['code'] == 200:
                eh.logger.success(f'Design Lead {first_name} {last_name} added to Middleware.')
            else:
                error = f'Error adding design lead {first_name} {last_name} to Middleware. \nQuery: {query}\nResponse: {response}'
                eh.error_handler.add_error_v(error=error, origin='insert_design_lead')

    class SMS:
        class TextMessage:
            """Work in Progress"""

            def __init__(
                self,
                to_phone,
                from_phone=None,
                cust_no=None,
                name=None,
                category=None,
                points=None,
                message=None,
                media=None,
                eh=ProcessOutErrorHandler,
            ):
                self.to_phone = to_phone
                self.from_phone = from_phone or creds.Twilio.phone_number
                self.cust_no = cust_no
                self.name = name or Database.CP.Customer.get_name(cust_no)
                self.first_name = self.name.split(' ')[0] or None
                self.category = category or Database.CP.Customer.get_category(cust_no)
                self.points = points or Database.CP.Customer.get_loyalty_balance(cust_no)
                self.message = message
                self.media = media  # URL of the media
                self.origin = None  # Origin of the text ('SERVER, AUTOMATIONS, MASS_CAMPAIGN, MESSENGER')
                self.campaign = None  # Campaign Name
                self.username = None  # Username of the user sending the text
                self.sid = None  # Twilio message response SID
                self.response_code = None  # Twilio message error code
                self.response_text: str = None  # Twilio message error text
                self.count = None
                self.test_mode = False
                self.eh = eh

            def __str__(self):
                return f'Customer {self.cust_no}: {self.name} - {self.phone}'

            def insert(self):
                verbose = False

                if self.name:
                    name = Database.sql_scrub(self.name)
                    name = name[:80]  # Truncate name to 80 characters

                if self.message:
                    body = Database.sql_scrub(self.message)
                    body = body[:1000]  # Truncate body to 1000 characters

                if self.response_text:
                    error_message = str(self.response_text)
                    error_message = Database.sql_scrub(error_message)
                    error_message = error_message[:255]

                if self.media:
                    media = self.media[:500]  # Truncate media to 500 characters

                to_phone = PhoneNumber(self.to_phone).to_cp()
                from_phone = PhoneNumber(self.from_phone).to_cp()

                if from_phone == PhoneNumber(creds.Twilio.phone_number).to_cp():
                    direction = 'OUTBOUND'
                else:
                    direction = 'INBOUND'

                query = f"""
                    INSERT INTO {creds.sms_table} (ORIGIN, CAMPAIGN, DIRECTION, TO_PHONE, FROM_PHONE, CUST_NO, NAME, BODY, 
                    USERNAME, CATEGORY, MEDIA, SID, ERROR_CODE, ERROR_MESSAGE)
                    VALUES ('{self.origin}', {f"'{self.campaign}'" if self.campaign else 'NULL'}, '{direction}', '{to_phone}', '{from_phone}', 
                    {f"'{self.cust_no}'" if self.cust_no else 'NULL'}, {f"'{name}'" if name else 'NULL'},'{body}', {f"'{self.username}'" if self.username else 'NULL'},
                    {f"'{self.category}'" if self.category else 'NULL'}, {f"'{media}'" if media else 'NULL'}, {f"'{self.sid}'" if self.sid else 'NULL'}, 
                    {f"'{self.response_code}'" if self.response_code else 'NULL'}, {f"'{error_message}'" if error_message else 'NULL'})
                    """

                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        if direction == 'OUTBOUND':
                            self.eh.logger.success(f'SMS sent to {to_phone} added to Database.')
                        else:
                            self.eh.tabase.logger.success(f'SMS received from {from_phone} added to Database.')
                else:
                    error = (
                        f'Error adding SMS sent to {to_phone} to Middleware. \nQuery: {query}\nResponse: {response}'
                    )
                    self.eh.error_handler.add_error_v(error=error, origin='insert_sms')

        @staticmethod
        def get(cust_no=None):
            if cust_no:
                query = f"""
                SELECT * FROM {Table.sms}
                WHERE CUST_NO = '{cust_no}'
                """
            else:
                query = f"""
                SELECT * FROM {Table.sms}
                """
            return Database.query(query)

        @staticmethod
        def insert(
            origin,
            to_phone,
            from_phone,
            cust_no,
            name,
            category,
            body,
            media,
            sid,
            error_code,
            error_message,
            campaign=None,
            username=None,
            eh=ProcessOutErrorHandler,
        ):
            body = Database.sql_scrub(body)
            body = body[:1000]  # 1000 char limit

            if name is not None:
                name = Database.sql_scrub(name)

            to_phone = PhoneNumber(to_phone).to_cp()
            from_phone = PhoneNumber(from_phone).to_cp()

            if from_phone == PhoneNumber(creds.Twilio.phone_number).to_cp():
                direction = 'OUTBOUND'
            else:
                direction = 'INBOUND'

            query = f"""
                INSERT INTO {Table.sms} (ORIGIN, CAMPAIGN, DIRECTION, TO_PHONE, FROM_PHONE, CUST_NO, BODY, USERNAME, NAME, CATEGORY, MEDIA, SID, ERROR_CODE, ERROR_MESSAGE)
                VALUES ('{origin}', {f"'{campaign}'" if campaign else 'NULL'}, '{direction}', '{to_phone}', '{from_phone}', 
                {f"'{cust_no}'" if cust_no else 'NULL'}, '{body}', {f"'{username}'" if username else 'NULL'}, '{name}', 
                {f"'{category}'" if category else 'NULL'}, {f"'{media}'" if media else 'NULL'}, {f"'{sid}'" if sid else 'NULL'}, 
                {f"'{error_code}'" if error_code else 'NULL'}, {f"'{error_message}'" if error_message else 'NULL'})
                """
            response = Database.query(query)
            if response['code'] == 200:
                if direction == 'OUTBOUND':
                    eh.logger.success(f'SMS sent to {to_phone} added to Database.')
                else:
                    eh.logger.success(f'SMS received from {from_phone} added to Database.')
            else:
                error = f'Error adding SMS sent to {to_phone} to Middleware. \nQuery: {query}\nResponse: {response}'
                eh.error_handler.add_error_v(error=error, origin='insert_sms')

        @staticmethod
        def move_phone_1_to_landline(origin, campaign, cust_no, name, category, phone, eh=ProcessOutErrorHandler):
            cp_phone = PhoneNumber(phone).to_cp()
            move_landline_query = f"""
                UPDATE AR_CUST
                SET MBL_PHONE_1 = '{cp_phone}', SET PHONE_1 = NULL
                WHERE PHONE_1 = '{cp_phone}'
            """
            response = Database.query(move_landline_query)

            if response['code'] == 200:
                query = f"""
                INSERT INTO {Table.sms_event} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME, CATEGORY, EVENT_TYPE, MESSAGE)
                VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}', 
                'Landline', 'SET MBL_PHONE_1 = {cp_phone}, SET PHONE_1 = NULL')"""

                response = Database.query(query)
                if response['code'] != 200:
                    eh.error_handler.add_error_v(f'Error moving {phone} to landline')

            else:
                eh.error_handler.add_error_v(f'Error moving {phone} to landline')

        @staticmethod
        def subscribe(phone, origin='SERVER', campaign='MISC', eh=ProcessOutErrorHandler):
            phone = PhoneNumber(phone).to_cp()
            subscriptions = Database.SMS.get_sms_subscriptions(phone)
            changes = False
            for x in subscriptions:
                if subscriptions[x]:
                    eh.logger.warn(f'{phone} already subscribed to {x}.')
                else:
                    if x == Table.CP.Customers.Column.sms_1_is_subscribed:
                        phone_column = Table.CP.Customers.Column.mobile_phone_1

                    elif x == Table.CP.Customers.Column.sms_2_is_subscribed:
                        phone_column = Table.CP.Customers.Column.mobile_phone_2
                    else:
                        continue

                    query = f"""
                    UPDATE AR_CUST
                    SET {x} = 'Y'
                    WHERE {phone_column} = '{phone}' and {x} = 'N'
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        changes = True
                        query = f"""
                        SELECT CUST_NO, NAM, CATEG_COD
                        FROM AR_CUST
                        WHERE {phone_column} = '{phone}'
                        """
                        response = Database.query(query, mapped=True)
                        if response['code'] == 200:
                            cust_no = response['data'][0]['CUST_NO']
                            name = response['data'][0]['NAM']
                            category = response['data'][0]['CATEG_COD']

                            query = f"""
                            INSERT INTO {Table.sms_event} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME,
                            CATEGORY, EVENT_TYPE, MESSAGE)

                            VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}',
                            'Subscribe', 'SET {x} = Y')"""
                            response = Database.query(query)
                            if response['code'] == 200:
                                eh.logger.success(f'Subscribed {phone_column}:{phone} to {x}')
                            else:
                                eh.error_handler.add_error_v(f'Error subscribing {phone} to SMS')

                    elif response['code'] == 201:
                        pass

                    else:
                        eh.error_handler.add_error_v(f'Error subscribing {phone} to SMS')

        @staticmethod
        def unsubscribe(phone, origin='SERVER', campaign='MISC', eh=ProcessInErrorHandler):
            phone = PhoneNumber(phone).to_cp()
            subscriptions = Database.SMS.get_sms_subscriptions(phone)
            changes = False
            if subscriptions:
                for x in subscriptions:
                    if subscriptions[x]:
                        if x == Table.CP.Customers.Column.sms_1_is_subscribed:
                            phone_column = Table.CP.Customers.Column.mobile_phone_1
                        elif x == Table.CP.Customers.Column.sms_2_is_subscribed:
                            phone_column = Table.CP.Customers.Column.mobile_phone_2
                        else:
                            continue
                        where = f"{phone_column} = '{phone}'"

                        query = f"""
                        UPDATE AR_CUST
                        SET {x} = 'N'
                        WHERE {where} and {x} = 'Y'
                        """
                        response = Database.query(query)
                        if response['code'] == 200:
                            changes = True
                            query = f"""
                            SELECT CUST_NO, NAM, CATEG_COD
                            FROM AR_CUST
                            WHERE {where}
                            """
                            response = Database.query(query, mapped=True)
                            if response['code'] == 200:
                                cust_no = response['data'][0]['CUST_NO']
                                name = response['data'][0]['NAM']
                                category = response['data'][0]['CATEG_COD']

                                query = f"""
                                INSERT INTO {Table.sms_event} (ORIGIN, CAMPAIGN, PHONE, CUST_NO, NAME,
                                CATEGORY, EVENT_TYPE, MESSAGE)

                                VALUES ('{origin}', '{campaign}', '{phone}', '{cust_no}', '{name}', '{category}',
                                'Unsubscribe', 'SET {x} = N')"""
                                response = Database.query(query)
                                if response['code'] == 200:
                                    eh.logger.success(f'Unsubscribed {phone_column}:{phone} from {x}')
                                else:
                                    eh.error_handler.add_error_v(f'Error unsubscribing {phone} from SMS')

                        else:
                            eh.error_handler.add_error_v(f'Error unsubscribing {phone} from SMS')

            if not changes:
                eh.logger.warn(f'{phone} not found in SMS subscriptions.')

        def get_sms_subscriptions(phone_number: str, eh=ProcessInErrorHandler) -> dict:
            """Returns True if phone number is subscribed to SMS notifications."""
            # Find the columns that contain the phone number
            result = {
                Table.CP.Customers.Column.sms_1_is_subscribed: False,
                Table.CP.Customers.Column.sms_2_is_subscribed: False,
            }
            phone_locations = Database.CP.Customer.find_phone(phone_number)

            if not phone_locations:
                return result

            for location in phone_locations:
                if location == Table.CP.Customers.Column.mobile_phone_1:
                    phone_column = Table.CP.Customers.Column.sms_1_is_subscribed

                elif location == Table.CP.Customers.Column.mobile_phone_2:
                    phone_column = Table.CP.Customers.Column.sms_2_is_subscribed

                else:
                    continue

                query = f"""
                SELECT {phone_column}
                FROM {Table.CP.Customers.table}
                WHERE {location} = '{phone_number}'
                """
                response = Database.query(query)

                try:
                    result[phone_column] = response[0][0] == 'Y'
                except:
                    pass

            return result

    class StockNotification:
        def has_info(item_no, email=None, phone=None):
            query = f"""
            SELECT ITEM_NO 
            FROM {Table.stock_notify}
            WHERE ITEM_NO = '{item_no}'
            """

            if email is not None:
                query += f" AND EMAIL = '{email}'"

            if phone is not None:
                query += f" AND PHONE = '{phone}'"

            try:
                response = Database.query(query)
                return response[0][0] is not None
            except:
                return False

        def insert(item_no, email=None, phone=None):
            cols = 'ITEM_NO'
            values = f"'{item_no}'"

            if email is not None:
                cols += ', EMAIL'
                values += f", '{email}'"

            if phone is not None:
                cols += ', PHONE'
                values += f", '{phone}'"

            query = f"""
            INSERT INTO {Table.stock_notify}
            ({cols})
            VALUES
            ({values})
            """

            response = Database.query(query)
            return response

    class Newsletter:
        def is_subscribed(email):
            """Returns True if email is subscribed to the generic newsletter via the website.
            If cp is True, checks if email is subscribed to the newsletter in Counterpoint."""
            if not email:
                return False
            cust_no = Database.CP.Customer.lookup_customer_by_email(email)
            if cust_no:
                # Check if email is subscribed to the newsletter in Counterpoint
                query = f"""
                SELECT {Table.CP.Customers.Column.email_1_is_subscribed}
                FROM {Table.CP.Customers.table}
                WHERE EMAIL_ADRS_1 = '{email}' or EMAIL_ADRS_2 = '{email}'
                """
                response = Database.query(query)
                try:
                    return response[0][0] == 'Y'
                except:
                    return False

            else:
                # Check if email is subscribed to the generic newsletter signup form
                query = f"""
                SELECT EMAIL, ENABLED
                FROM {Table.newsletter}
                WHERE EMAIL = '{email}'
                """
                response = Database.query(query)
                if response:
                    try:
                        return response[0][1] == 1
                    except:
                        return False

        def insert(email, date=None, eh=ProcessOutErrorHandler):
            """Inserts an email into the newsletter subscriber table."""
            if not date:
                query = f"""
                INSERT INTO {Table.newsletter} (EMAIL)
                VALUES ('{email}')"""
            else:
                query = f"""
                INSERT INTO {Table.newsletter} (EMAIL, CREATED_DT)
                VALUES ('{email}', '{date}')"""
            response = Database.query(query)
            return response

        def unsubscribe(email, eh=ProcessOutErrorHandler):
            if not Database.Newsletter.is_subscribed(email):
                return {'code': 201, 'message': f'{email} not found in newsletter table.'}

            # Unsubscribe from Counterpoint
            query = f"""
            UPDATE {Table.CP.Customers.table}
            SET {Table.CP.Customers.Column.email_1_is_subscribed} = 'N'
            WHERE EMAIL_ADRS_1 = '{email}' or EMAIL_ADRS_2 = '{email}'
            """
            response = Database.query(query)
            if response['code'] == 200:
                # Email was found in Counterpoint and updated to unsubscribed
                eh.logger.success(
                    f'Unsubscribed {email} from newsletter.', origin='Database.Newsletter.unsubscribe'
                )
            elif response['code'] == 201:
                # Email not found in Counterpoint. Check if email is in newsletter table.
                query = f"""
                UPDATE {Table.newsletter}
                SET ENABLED = 0
                WHERE EMAIL = '{email}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(
                        f'Unsubscribed {email} from newsletter.', origin='Database.Newsletter.unsubscribe'
                    )
                elif response['code'] == 201:
                    eh.logger.warn(
                        f'{email} not found in newsletter table.', origin='Database.Newsletter.unsubscribe'
                    )
                else:
                    error = f'Error unsubscribing {email} from newsletter. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)
            else:
                error = f'Error unsubscribing {email} from newsletter. \n Query: {query}\nResponse: {response}'
                eh.error_handler.add_error_v(error=error)
                raise Exception(error)

            return response

        def subscribe(email, cust_no=None, eh=ProcessOutErrorHandler):
            """Subscribes an email to the newsletter in Counterpoint (current customer) or
            the newsletter table if this is not a current customer."""
            if Database.Newsletter.is_subscribed(email):
                return {'code': 201, 'message': f'{email} already subscribed to newsletter.'}

            # Subscribe in Counterpoint
            query = f"""
            UPDATE {Table.CP.Customers.table}
            SET {Table.CP.Customers.Column.email_1_is_subscribed} = 'Y'
            WHERE EMAIL_ADRS_1 = '{email}' or EMAIL_ADRS_2 = '{email}'
            """
            response = Database.query(query)

            if response['code'] == 200:
                # Email was found in Counterpoint and updated to subscribed
                eh.logger.success(f'Subscribed {email} to newsletter.')

            elif response['code'] == 201:
                # Email not found in Counterpoint.
                if cust_no:
                    # If cust_no is provided, add email to customer record.
                    response = Database.CP.Customer.add_email(email, cust_no)
                    if response['code'] == 200:
                        eh.logger.success(
                            f'Subscribed {email} to newsletter.', origin='Database.Newsletter.subscribe'
                        )
                else:
                    response = Database.Newsletter.insert(email, eh=eh)
                    if response['code'] == 200:
                        eh.logger.success(
                            f'Subscribed {email} to newsletter.', origin='Database.Newsletter.subscribe'
                        )
            else:
                error = f'Error subscribing {email} to newsletter. \n Query: {query}\nResponse: {response}'
                eh.error_handler.add_error_v(error=error)
                raise Exception(error)

            return response

    class CP:
        """Methods for interacting with the Counterpoint database."""

        class GiftCard:
            def exists(code) -> bool:
                query = f"""
                        SELECT GFC_NO FROM SY_GFC
                        WHERE GFC_NO = '{code}'
                        """

                response = Database.query(query)
                try:
                    return response[0][0] is not None
                except:
                    return False

            def create_code() -> str:
                code_gen = ShortUUID()
                code_gen.set_alphabet('ABCDEFG123456789')  # 16
                code = code_gen.random(12)
                code = f'{code[0:4]}-{code[4:8]}-{code[8:12]}'

                if Database.CP.GiftCard.exists(code):
                    return Database.CP.GiftCard.create_code()
                else:
                    return code

            def insert(
                doc_id: str,
                pay_code: str,
                card_no: str,
                amount: float,
                lin_seq_no: int,
                descr: str,
                gfc_seq_no: int,
                store_credit: bool = False,
                eh=ProcessInErrorHandler,
            ):
                """Inserts a gift card into the Counterpoint database."""
                store_credit = 'Y' if store_credit else 'N'

                query = f"""
                INSERT INTO PS_DOC_GFC
                (DOC_ID, GFC_COD, GFC_NO, AMT, LIN_SEQ_NO, DESCR, CREATE_AS_STC, GFC_SEQ_NO)
                VALUES
                ('{doc_id}', '{pay_code}', '{card_no}', {amount}, {lin_seq_no}, '{descr}', 
                '{store_credit}', {gfc_seq_no})
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Gift card {card_no} written for ${amount}')
                else:
                    eh.error_handler.add_error_v(f'Gift card {card_no} could not be written')
                    eh.error_handler.add_error_v(response['message'])

            def get_balance(card_no: str) -> float:
                query = f"""
                    SELECT CURR_AMT 
                    FROM SY_GFC
                    WHERE GFC_NO = '{card_no}'
                    """

                response = Database.query(query)
                try:
                    return float(response[0][0])
                except:
                    return 0

            def get_next_seq_no(gfc_no: str) -> int:
                query = f"""
                    SELECT MAX(SEQ_NO) 
                    FROM SY_GFC_ACTIV
                    WHERE GFC_NO = '{gfc_no}'
                    """

                response = Database.query(query)

                try:
                    return int(response[0][0]) + 1
                except:
                    return 1

            def insert_activity(
                tkt_no: str,
                card_no: str,
                amount: float,
                doc_id: str,
                store: str,
                station: str,
                activity: str = 'R',  # R = Redeem
                user_id: str = 'POS',
                eh=ProcessInErrorHandler,
            ):
                current_date = datetime.now().strftime('%Y-%m-%d')
                seq_no = Database.CP.GiftCard.get_next_seq_no(card_no)

                query = f"""
                        INSERT INTO SY_GFC_ACTIV
                        (GFC_NO, SEQ_NO, DAT, STR_ID, 
                        STA_ID, DOC_NO, ACTIV_TYP, AMT, 
                        LST_MAINT_DT, LST_MAINT_USR_ID, DOC_ID)
                        
                        VALUES
                        ('{card_no}', {seq_no}, '{current_date}', '{store}', 
                        '{station}', '{tkt_no}', '{activity}', {amount}, 
                        GETDATE(), '{user_id}', '{doc_id}')
                        """
                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Activty {activity} for ${amount} on {card_no} written')
                else:
                    eh.error_handler.add_error_v(
                        f'Activity {activity} for ${amount} on {card_no} could not be written'
                    )
                    eh.error_handler.add_error_v(response['message'])

            def update_balance(card_no: str, amount: float, eh=ProcessInErrorHandler):
                query = f"""
                        UPDATE SY_GFC
                        SET CURR_AMT = {amount}
                        WHERE GFC_NO = '{card_no}'
                        """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success('Gift card balance updated')
                else:
                    eh.error_handler.add_error_v('Gift card balance could not be updated')
                    eh.error_handler.add_error_v(response['message'])

            def get_last_activity_index(card_no: str) -> int:
                query = f"""
                SELECT MAX(SEQ_NO) FROM SY_GFC_ACTIV
                WHERE GFC_NO = '{card_no}'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0])
                except:
                    return 1

            def update_activity(card_no: str, tkt_no: str, amt_spent: float, eh=ProcessInErrorHandler):
                query = f"""
                UPDATE SY_GFC_ACTIV
                SET AMT = {amt_spent},
                DOC_NO = '{tkt_no}'
                WHERE GFC_NO = '{card_no}' AND SEQ_NO = {Database.CP.GiftCard.get_last_activity_index()}
                """

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'TKT_NO: {tkt_no}: Gift card activity updated ${amt_spent}')
                else:
                    eh.error_handler.add_error_v('Gift card activity could not be updated')
                    eh.error_handler.add_error_v(response['message'])

        class Loyalty:
            def write_line(doc_id: int, lin_seq_no: int, points_earned: int, eh=ProcessInErrorHandler):
                query = f"""
                INSERT INTO PS_DOC_LIN_LOY 
                (DOC_ID, LIN_SEQ_NO, LIN_LOY_PTS_EARND, LOY_PGM_RDM_ELIG, LOY_PGM_AMT_PD_WITH_PTS, LOY_PT_EARN_RUL_DESCR, LOY_PT_EARN_RUL_SEQ_NO) 
                VALUES 
                ('{doc_id}', {lin_seq_no}, {points_earned}, 'Y', 0, 'Basic', 5)
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Line loyalty points ({points_earned}) for line {lin_seq_no} written')
                else:
                    eh.error_handler.add_error_v(f'Line #{lin_seq_no} could not receive loyalty points')

            def get_points_used(doc_id, eh=ProcessInErrorHandler) -> int:
                """Returns the total number of loyalty points used in a Counterpoint order."""
                query = f"""
                SELECT AMT FROM PS_DOC_PMT
                WHERE PAY_COD = 'LOYALTY' AND DOC_ID = '{doc_id}'
                """

                response = Database.query(query)
                points_used = 0
                try:
                    points_used = math.floor(float(response[0][0])) if response else 0
                except:
                    pass
                return points_used

            def write_ps_doc_hdr_loy_pgm(
                doc_id, points_earned: float, points_redeemed: float, point_balance: float, eh=ProcessInErrorHandler
            ):
                query = f"""
                INSERT INTO PS_DOC_HDR_LOY_PGM
                (DOC_ID, LIN_LOY_PTS_EARND, LOY_PTS_EARND_GROSS, LOY_PTS_ADJ_FOR_RDM, LOY_PTS_ADJ_FOR_INC_RND, LOY_PTS_ADJ_FOR_OVER_MAX, LOY_PTS_EARND_NET, LOY_PTS_RDM, LOY_PTS_BAL)
                VALUES
                ('{doc_id}', 0, 0, 0, 0, 0, {points_earned}, {points_redeemed}, {point_balance})
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(
                        f'Loyalty points written to PS_DOC_HDR_LOY_PGM. Earned: {points_earned}, Redeemed: {points_redeemed}, Balance: {point_balance}'
                    )
                else:
                    eh.error_handler.add_error_v('Loyalty points could not be written')

            def redeem(amount: int, cust_no: str, eh=ProcessInErrorHandler):
                """Subtracts loyalty points from a customer's account."""
                query = f"""
                UPDATE {creds.Table.CP.Customers.table}
                SET LOY_PTS_BAL = LOY_PTS_BAL - {amount}
                WHERE CUST_NO = '{cust_no}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'{amount} Loyalty points subtracted from {cust_no}')
                else:
                    eh.error_handler.add_error_v('Loyalty points could not be subtracted')
                    eh.error_handler.add_error_v(response['message'])

            def add_points(amount: int, cust_no: str, eh=ProcessInErrorHandler):
                """Adds loyalty points to a customer's account."""
                query = f"""
                UPDATE {creds.Table.CP.Customers.table}
                SET LOY_PTS_BAL = LOY_PTS_BAL + {amount}
                WHERE CUST_NO = '{cust_no}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'{amount} Loyalty points added from {cust_no}')
                else:
                    eh.error_handler.add_error_v('Loyalty points could not be added')
                    eh.error_handler.add_error_v(response['message'])

        class OpenOrder:
            def set_value(table, column, value, index, doc_id, eh=ProcessInErrorHandler):
                query = f"""
                    UPDATE {table}
                    SET {column} = {value}
                    WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                    """
                r = Database.query(query)
                if r['code'] == 200:
                    eh.logger.success(f'[{table}] Line {index} {column} set to {value}')
                else:
                    eh.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be set to {value}')
                    eh.error_handler.add_error_v(r['message'])

            def get_value(table, column, index, doc_id, eh=ProcessInErrorHandler):
                query = f"""
                SELECT {column} FROM {table}
                WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {index}
                """

                response = Database.query(query)

                try:
                    return float(response[0][0]) if response else None
                except Exception as e:
                    eh.error_handler.add_error_v(f'[{table}] Line {index} {column} could not be retrieved')
                    raise e

            def delete(doc_id: str = None, tkt_no: str = None, orig_doc: bool = False, eh=ProcessOutErrorHandler):
                """Deletes an order from the PS_DOC_HDR table. If orig_doc is True, deletes from PS_DOC_HDR_ORIG_DOC."""
                if not doc_id and not tkt_no:
                    eh.error_handler.add_error_v('No doc_id or tkt_no provided to delete order.')
                    return
                if orig_doc:
                    table = 'PS_DOC_HDR_ORIG_DOC'
                else:
                    table = 'PS_DOC_HDR'

                if doc_id:
                    column = 'DOC_ID'
                    value = doc_id
                elif tkt_no:
                    if orig_doc:
                        eh.error_handler.add_error_v('Cannot delete original document by tkt_no.')
                        return
                    column = 'TKT_NO'
                    value = tkt_no

                query = f"""
                DELETE FROM {table}
                WHERE {column} = '{value}'
                """

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Order {doc_id or tkt_no} deleted from {table}.')
                elif response['code'] == 201:
                    eh.logger.warn(f'Order {doc_id or tkt_no} not found in {table}.')
                else:
                    error = f'Error deleting order {doc_id or tkt_no} from {table}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def has_refund(order_number) -> bool:
                """returns true if order has an associated refund"""
                query = f"""
                SELECT TKT_NO
                FROM {Table.CP.open_orders}
                WHERE TKT_NO like '{order_number}%' AND TKT_NO like '%R%'
                """
                response = Database.query(query)
                try:
                    return response[0][0] is not None
                except:
                    return False

            def tkt_num_exists(self, tkt_num: str, suffix: str = '', index: int = 1) -> bool:
                """Returns true if the provided ticket number exists in the PS_DOC_HDR table."""

                query = f"""
                SELECT TKT_NO 
                FROM PS_DOC_HDR
                WHERE TKT_NO like '{tkt_num}{suffix}{index}'
                """

                response = Database.query(query)

                try:
                    ticket_amt = len(response)

                    if ticket_amt == 0:
                        return False
                    else:
                        return True
                except:
                    return False

            def write_ticket_number(doc_id: str, tkt_no: str, eh=ProcessInErrorHandler):
                tables = ['PS_DOC_HDR', 'PS_DOC_LIN', 'PS_DOC_PMT']
                for table in tables:
                    query = f"""
                    UPDATE {table}
                    SET TKT_NO = '{tkt_no}'
                    WHERE DOC_ID = '{doc_id}'
                    """

                    response = Database.query(query)

                    if response['code'] == 200:
                        eh.logger.success('Ticket number updated.')
                    elif response['code'] == 201:
                        eh.logger.warn(f'DOC ID not found in {table}.')
                    else:
                        eh.error_handler.add_error_v(f'Ticket number could not be updated in {table}.')
                        eh.error_handler.add_error_v(response['message'])

            def update_payment_amount(doc_id: str, amount: float, pay_code: str = None, eh=ProcessInErrorHandler):
                pay_code_filter = f"AND PAY_COD = '{pay_code}'" if pay_code else ''

                query = f"""
                UPDATE PS_DOC_PMT
                SET AMT = {amount},
                HOME_CURNCY_AMT = {amount} 
                WHERE DOC_ID = '{doc_id} {pay_code_filter}'
                """

                response = Database.query(query)
                ending_text = (
                    f'for DOC_ID: {doc_id} and PAY_COD: {pay_code}' if pay_code else f' for DOC_ID: {doc_id}'
                )
                if response['code'] == 200:
                    eh.logger.success(f'Payment amount updated {ending_text}')
                elif response['code'] == 201:
                    eh.logger.warn(f'DOC ID {doc_id} not found in PS_DOC_PMT.')
                else:
                    eh.error_handler.add_error_v(f'Payment amount could not be updated {ending_text}')
                    eh.error_handler.add_error_v(response['message'])

            def update_payment_apply(doc_id: str, amount: float, pay_code: str = None, eh=ProcessInErrorHandler):
                pay_code_filter = f"AND PAY_COD = '{pay_code}'" if pay_code else ''
                query = f"""
                    UPDATE PS_DOC_PMT_APPLY
                    SET AMT = {amount},
                    HOME_CURNCY_AMT = {amount}
                    WHERE DOC_ID = '{doc_id}' AND PMT_SEQ_NO in (
                        SELECT PMT_SEQ_NO FROM PS_DOC_PMT WHERE DOC_ID = '{doc_id}' {pay_code_filter}
                    )
                    """

                r = Database.query(query)
                ending_text = (
                    f'for DOC_ID: {doc_id} and PAY_COD: {pay_code}' if pay_code else f' for DOC_ID: {doc_id}'
                )
                if r['code'] == 200:
                    eh.logger.success(f'Payment applied updated {ending_text}')
                else:
                    eh.error_handler.add_error_v(f'Payment applied could not be updated {ending_text}')
                    eh.error_handler.add_error_v(r['message'])

            def get_total(doc_id: str, eh=ProcessInErrorHandler) -> float:
                query = f"""
                SELECT SUM(EXT_PRC) FROM PS_DOC_LIN
                WHERE DOC_ID = '{doc_id}'
                """

                response = Database.query(query)

                try:
                    return abs(float(response[0][0]))
                except:
                    return 0

            def get_payment_by_code(doc_id: str, pay_code: str, eh=ProcessInErrorHandler) -> float:
                query = f"""
                SELECT AMT FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = '{pay_code}'
                """

                response = Database.query(query)

                try:
                    return abs(float(response[0][0]))
                except:
                    return 0

            def has_loyalty_payment(doc_id: str) -> bool:
                """Returns True if the provided document ID has a loyalty payment."""
                query = f"""
                SELECT COUNT(*) FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'LOYALTY'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0]) > 0
                except:
                    return False

            def has_gc_payment(doc_id: str) -> bool:
                query = f"""
                SELECT COUNT(*) FROM PS_DOC_PMT
                WHERE DOC_ID = '{doc_id}' AND PAY_COD = 'GC'
                """

                response = Database.query(query)

                try:
                    return int(response[0][0]) > 0
                except:
                    return False

            def set_ticket_date(doc_id: str, date: datetime, eh=ProcessInErrorHandler):
                date_string = date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                query = f"""
                UPDATE PS_DOC_HDR
                SET TKT_DT = '{date_string}'
                WHERE DOC_ID = '{doc_id}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success('Date updated')
                else:
                    eh.error_handler.add_error_v('Date could not be updated')
                    eh.error_handler.add_error_v(response['message'])

            def delete_hdr_total_entry(doc_id: str, eh=ProcessInErrorHandler):
                query = f"""
                DELETE FROM PS_DOC_HDR_TOT
                WHERE DOC_ID = '{doc_id}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success('Total removed')
                else:
                    eh.error_handler.add_error_v('Total could not be removed')
                    eh.error_handler.add_error_v(response['message'])

            def insert_hdr_total_entry(
                doc_id: str,
                lines: int,
                gfc_amt: float,
                sub_tot: float,
                tot_ext_cost: float,
                tot_tender: float,
                tot: float,
                total_hdr_disc: float,
                total_lin_disc: float,
                eh=ProcessInErrorHandler,
            ):
                query = f"""
                INSERT INTO PS_DOC_HDR_TOT
                (DOC_ID, TOT_TYP, INITIAL_MIN_DUE, HAS_TAX_OVRD, TAX_AMT_SHIPPED, LINS, TOT_GFC_AMT, 
                TOT_SVC_AMT, SUB_TOT, TAX_OVRD_LINS, TOT_EXT_COST, TOT_MISC, TAX_AMT, NORM_TAX_AMT, 
                TOT_TND, TOT_CHNG, TOT_WEIGHT, TOT_CUBE, TOT, AMT_DUE, TOT_HDR_DISC, TOT_LIN_DISC, 
                TOT_HDR_DISCNTBL_AMT, TOT_TIP_AMT)
                VALUES
                ('{doc_id}', 'S', 0, '!', 0, {lines}, {gfc_amt}, 
                0, {sub_tot}, 0, {tot_ext_cost}, 0, 0, 0, 
                {tot_tender}, 0, 0, 0, {tot}, 0, {total_hdr_disc}, {total_lin_disc}, 
                {sub_tot}, 0)
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success('Total written')
                else:
                    eh.error_handler.add_error_v('Total could not be written')
                    eh.error_handler.add_error_v(response['message'])

            def set_loyalty_program(doc_id: str, program_code: str = 'BASIC', eh=ProcessInErrorHandler):
                query = f"""
                UPDATE PS_DOC_HDR
                SET LOY_PGM_COD = '{program_code}'
                WHERE DOC_ID = '{doc_id}'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Loyalty program code updated to {program_code} for DOC_ID: {doc_id}')
                elif response['code'] == 201:
                    eh.logger.warn(f'DOC ID {doc_id} not found in PS_DOC_HDR.')
                else:
                    eh.error_handler.add_error_v(f'Doc ID {doc_id} could not be updated to {program_code}')
                    eh.error_handler.add_error_v(response['message'])

            def set_line_type(doc_id: str, line_type: str, eh=ProcessInErrorHandler) -> None:
                """Updates the line type for all lines in a given document."""
                if line_type not in ['S', 'R']:
                    raise Exception('Invalid line type. Must be "S" or "R"')

                query = f"""
                UPDATE PS_DOC_LIN
                SET LIN_TYP = '{line_type}'
                WHERE DOC_ID = '{doc_id}'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Updated line types to {line_type} for DOC_ID: {doc_id}')
                elif response['code'] == 201:
                    eh.logger.info(f'No lines to update for Doc ID: {doc_id}')
                else:
                    eh.error_handler.add_error_v(f'Line types could not be updated for DOC_ID: {doc_id}')

            def get_orig_doc_id(doc_id: str) -> str:
                """Returns the original document ID for a given document ID."""

                query = f"""
                SELECT ORIG_DOC_ID 
                FROM PS_DOC_HDR_ORIG_DOC 
                WHERE DOC_ID = '{doc_id}'
                """
                response = Database.query(query)
                try:
                    return response[0][0]
                except:
                    return None

            def set_apply_type(doc_id: str, apply_type: str = 'S', eh=ProcessInErrorHandler):
                query = f"""
                UPDATE PS_DOC_PMT_APPLY
                SET APPL_TYP = '{apply_type}'
                WHERE DOC_ID = '{doc_id}'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Updated payment application types to {apply_type} for DOC_ID: {doc_id}')
                elif response['code'] == 201:
                    eh.logger.info(f'No payment applications found to update for DOC_ID: {doc_id}')
                else:
                    eh.error_handler.add_error_v(
                        f'Payment application types could not be updated for DOC_ID: {doc_id}'
                    )
                    eh.error_handler.add_error_v(response['message'])

            def set_line_totals(
                doc_id: str,
                number_of_lines: int,
                line_total: float,
                is_refund: bool = False,
                eh=ProcessInErrorHandler,
            ):
                table = 'RET_LINS' if is_refund else 'SAL_LINS'
                to_release_lines = f', TO_REL_LINS = {number_of_lines}' if not is_refund else ''
                line_tot = f', RET_LIN_TOT = {line_total}' if is_refund else f'SAL_LIN_TOT = {line_total} '

                query = f"""
                UPDATE PS_DOC_HDR
                SET {table} = {number_of_lines} {to_release_lines}, {line_tot}
                WHERE DOC_ID = '{doc_id}'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success(f'Updated line items to {number_of_lines} for DOC_ID: {doc_id}')
                elif response['code'] == 201:
                    eh.logger.info(f'No lines to update for DOC_ID: {doc_id}')
                else:
                    eh.error_handler.add_error_v(
                        f'Line items could not be updated for DOC_ID: {doc_id}.\nResponse: {response}\nQuery: {query}'
                    )

            def set_ps_doc_lin_quantities(
                doc_id: str, qty: float, ext_prc: float, ext_cost: float, lin_seq_no: int, eh=ProcessInErrorHandler
            ):
                query = f"""
                    UPDATE PS_DOC_LIN
                    SET QTY_SOLD = {qty},
                    EXT_PRC = {ext_prc},
                    EXT_COST = {ext_cost}
                    WHERE DOC_ID = '{doc_id}' AND LIN_SEQ_NO = {lin_seq_no}
                    """
                r = Database.query(query)

                if r['code'] == 200:
                    eh.logger.success(f'Doc ID: {doc_id} Line {lin_seq_no} updated')
                elif r['code'] == 201:
                    eh.logger.warn(f'Doc ID: {doc_id} Line {lin_seq_no} not found')
                else:
                    eh.error_handler.add_error_v(f'Doc ID: {doc_id} Line {lin_seq_no} could not be updated')
                    eh.error_handler.add_error_v(r['message'])

            def get_ps_doc_pmt_index(pay_cod: str, payments: list[dict]) -> int:
                for index, pmt in enumerate(payments):
                    if pmt['PAY_COD'] == pay_cod:
                        return index
                return -1

        class ClosedOrder:
            def get_refund_customers(date):
                """Returns a list of customers who have a refunded ticket on a given date"""
                query = f"""
                SELECT CUST_NO
                FROM {Table.CP.closed_orders}
                WHERE BUS_DAT = '{date}' AND TKT_NO like '%R%'
                """
                response = Database.query(query)
                if response is not None:
                    return [x[0] for x in response]
                else:
                    return None

            def get_business_date(tkt_no=None, doc_id=None, eh=ProcessOutErrorHandler):
                if not tkt_no and not doc_id:
                    raise Exception('No Ticket Number or Document ID provided.')
                if doc_id:
                    where_filter = f'DOC_ID = {doc_id}'

                elif tkt_no:
                    where_filter = f"TKT_NO = '{tkt_no}'"

                query = f"""
                SELECT BUS_DAT
                FROM {Table.CP.closed_orders}
                WHERE {where_filter}
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    eh.logger.warn(f'No business date found for {tkt_no or doc_id}.')
                    return None

            def get_total(tkt_no, eh=ProcessOutErrorHandler):
                query = f"""
                SELECT TOT
                FROM {Table.CP.closed_orders}
                WHERE TKT_NO = '{tkt_no}'
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    eh.logger.warn(f'No total found for {tkt_no}.')
                    return None

            def has_refund(order_number) -> bool:
                """returns true if order has an associated refund"""
                query = f"""
                SELECT TKT_NO
                FROM {Table.CP.closed_orders}
                WHERE TKT_NO like '{order_number}%' AND TKT_NO like '%R%'
                """
                response = Database.query(query)
                try:
                    return response[0][0] is not None
                except:
                    return False

            def get_last_successful_order(customer):
                """Find the last order that does not have an associated refund"""
                refunded = True
                count = 0
                while refunded:
                    query = f"""
                    SELECT TKT_NO FROM {Table.CP.closed_orders}
                    WHERE CUST_NO = '{customer}' AND TKT_NO not like '%R%'
                    ORDER BY BUS_DAT DESC
                    OFFSET {count} ROWS
                    FETCH NEXT 1 ROWS ONLY
                    """
                    response = Database.query(query)
                    if response is not None:
                        most_recent_ticket = response[0][0]
                        if Database.CP.ClosedOrder.has_refund(most_recent_ticket):
                            # Evaluate whether this is full or partial refund...
                            count += 1
                        else:
                            # Return the most recent ticket that does not have a refund
                            return most_recent_ticket
                    else:
                        return None

            def get_first_successful_order(customer):
                """Find the first order that does not have an associated refund"""
                refunded = True
                count = 0
                while refunded:
                    query = f"""
                    SELECT TKT_NO FROM {Table.CP.closed_orders}
                    WHERE CUST_NO = '{customer}' AND TKT_NO not like '%R%'
                    ORDER BY BUS_DAT
                    OFFSET {count} ROWS
                    FETCH NEXT 1 ROWS ONLY
                    """
                    response = Database.query(query)
                    if response is not None:
                        first_ticket = response[0][0]
                        if Database.CP.ClosedOrder.has_refund(first_ticket):
                            count += 1
                        else:
                            # Return the most recent ticket that does not have a refund
                            return first_ticket
                    else:
                        return None

        class Product:
            def get_binding_id(item_no=None):
                """Returns the binding id of a sku, or a list of unique and validated binding IDs
                from the ITEM table."""
                if item_no:
                    query = f"""
                    SELECT {Table.CP.Item.Column.binding_id}
                    FROM {Table.CP.Item.table}
                    WHERE ITEM_NO = '{item_no}'
                    """
                    response = Database.query(query=query)
                    try:
                        return response[0][0]
                    except:
                        return None
                else:
                    response = Database.query(
                        f'SELECT DISTINCT {Table.CP.Item.Column.binding_id} '
                        f"FROM {Table.CP.Item.table} WHERE {Table.CP.Item.Column.web_enabled} = 'Y'"
                        f'AND {Table.CP.Item.Column.binding_id} IS NOT NULL'
                    )

                    def valid(binding_id):
                        return re.match(creds.Company.binding_id_format, binding_id)

                    return [binding[0] for binding in response if valid(binding[0])] if response else []

            def get_by_category(category):
                query = f"""
                SELECT * FROM {Table.CP.Item.table}
                WHERE CATEG_COD = '{category}'
                """
                return Database.query(query, mapped=True)

            def get_cost(item_no: str, eh=ProcessInErrorHandler) -> float:
                """Returns the cost of an item from the IM_ITEM table."""
                cost: float = 0

                query = f"""
                SELECT LST_COST 
                FROM IM_ITEM
                WHERE ITEM_NO = '{item_no}'
                """

                response = Database.query(query)
                try:
                    cost = float(response[0][0])
                except Exception as e:
                    eh.error_handler.add_error_v(f'Could not get cost for {item_no}')
                    eh.error_handler.add_error_v(str(e))

                return cost

            def get_total_sold(start_date, end_date, item_no):
                query = f"""
                SELECT sum(QTY_SOLD) AS '2022 QTY SOLD' from PS_TKT_HIST_LIN
                WHERE ITEM_NO IN (SELECT ITEM_NO 
                FROM IM_ITEM
                WHERE ITEM_NO = '{item_no}'
                and BUS_DAT >= '{start_date}' AND BUS_DAT < '{end_date}'"""

                response = Database.query(query)
                if response:
                    return response[0][0]

            def get_single_items():
                query = f"""
                SELECT ITEM_NO FROM {Table.CP.Item.table}
                WHERE {Table.CP.Item.Column.web_enabled} = 'Y' 
                AND {Table.CP.Item.Column.binding_id} IS NULL
                """
                response = Database.query(query)
                if response:
                    return [x[0] for x in response]
                else:
                    return None

            def set_sale_price(sku, price, verbose=False, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE {Table.CP.item_prices}
                SET PRC_2 = {price}, LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO = '{sku}'				
                """
                response = Database.query(query)
                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Sale Price set for {sku}.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Set Sale Price: No rows affected for {sku}.')
                else:
                    print(response['code'])
                    error = f'Error setting sale price for {sku}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def remove_sale_price(items, verbose=False, eh=ProcessOutErrorHandler):
                if len(items) > 1:
                    where_filter = f'WHERE ITEM_NO IN {tuple(items)}'
                else:
                    where_filter = f"WHERE ITEM_NO = '{items[0]}'"

                query = f"""
                UPDATE IM_PRC
                SET PRC_2 = NULL, LST_MAINT_DT = GETDATE()
                {where_filter}
        
                UPDATE IM_ITEM
                SET IS_ON_SALE = 'N', SALE_DESCR = NULL, LST_MAINT_DT = GETDATE()
                {where_filter}
                """
                # Removing Sale Price, Last Maintenance Date, and Removing from On Sale Category
                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Sale Price removed successfully from {items}.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'No Rows Affected for {items}')
                else:
                    eh.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Removal")'
                    )

            def set_sale_status(
                items: list, status: bool, description=None, verbose=False, eh=ProcessOutErrorHandler
            ):
                if not items:
                    raise Exception('No items provided to set sale status.')

                where_filter = f""" WHERE ITEM_NO IN ({','.join([f"'{x}'" for x in items])})"""

                query = f"""
                    UPDATE {Table.CP.Item.table}
                    SET IS_ON_SALE = '{"Y" if status else "N"}', LST_MAINT_DT = GETDATE()
                    """
                if description:
                    query += f", SALE_DESCR = '{description}'"
                else:
                    query += ', SALE_DESCR = NULL'

                query += where_filter

                # Updating Sale Price, Sale Flag, Sale Description, Last Maintenance Date
                response = Database.query(query)
                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Sale status updated for {items}.')
                    if status:
                        Database.CP.Product.add_to_sale_category(sku_list=items)
                    else:
                        Database.CP.Product.remove_from_sale_category(sku_list=items)

                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Sale status not updated for {items}.')
                else:
                    error = f'Error updating sale status for {items}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def set_active(sku, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE {Table.CP.Item.table} 
                SET STAT = 'A'
                WHERE ITEM_NO = '{sku}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Product {sku} set to active status.')
                elif response['code'] == 201:
                    eh.logger.warn(f'Product {sku} not found in {Table.CP.Item.table}.')
                else:
                    error = f'Error setting product {sku} to active status. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def add_to_sale_category(sku_list: list, eh=ProcessOutErrorHandler):
                if not sku_list:
                    raise Exception('No SKU provided to add to sale category.')
                success = 0
                fail = 0
                for x in sku_list:
                    query = f"""
                    INSERT INTO EC_CATEG_ITEM(ITEM_NO, CATEG_ID, ENTRY_SEQ_NO, LST_MAINT_DT, LST_MAINT_USR_ID)
                    VALUES('{x}', '{creds.Counterpoint.Categories.on_sale}', '1', GETDATE(), 'POS')
                    """

                    response = Database.query(query)
                    if response['code'] == 200:
                        success += 1
                    elif response['code'] == '23000':
                        pass
                    else:
                        fail += 1
                        error = (
                            f'Error adding product {x} to sale category. \n Query: {query}\nResponse: {response}'
                        )
                        eh.error_handler.add_error_v(error=error)
                if success:
                    eh.logger.success(f'{success} products added to sale category.')
                if fail:
                    eh.logger.warn(f'{fail} products not added to sale category.')

            def remove_from_sale_category(sku_list: list, eh=ProcessOutErrorHandler):
                query = f"""
                DELETE FROM EC_CATEG_ITEM
                WHERE ITEM_NO in ({','.join([f"'{x}'" for x in sku_list])}) AND CATEG_ID = '{creds.Counterpoint.Categories.on_sale}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Products: {sku_list} removed from sale category.')
                elif response['code'] == 201:
                    eh.logger.warn(f'Products: {sku_list} not found in sale category.')
                else:
                    error = f'Error removing products {sku_list} from sale category. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def add_to_new(sku_list: list, verbose=False, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE IM_ITEM
                SET ECOMM_NEW = 'Y', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO in ({','.join([f"'{x}'" for x in sku_list])})
                AND ECOMM_NEW = 'N'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Products: {sku_list} updated to NEW.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Products: {sku_list} not found.')
                else:
                    error = f'Error updating products {sku_list}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def remove_from_new(sku_list: list, verbose=False, eh=ProcessOutErrorHandler):
                """Takes in a list of new items and sets opposing items to 'N'"""
                query = f"""
                UPDATE IM_ITEM
                SET ECOMM_NEW = 'N', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO not in ({','.join([f"'{x}'" for x in sku_list])}) AND
                ECOMM_NEW = 'Y'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Products: {sku_list} updated to NEW.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Products: {sku_list} not found.')
                else:
                    error = f'Error updating products {sku_list}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def add_to_back_in_stock(sku_list: list, verbose=False, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE IM_ITEM
                SET IS_BACK_IN_STOCK = 'Y', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO in ({','.join([f"'{x}'" for x in sku_list])})
                AND IS_BACK_IN_STOCK = 'N'

                """

                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Products: {sku_list} updated to BACK IN STOCK.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Products: {sku_list} not found.')
                else:
                    error = f'Error updating products {sku_list}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def remove_from_back_in_stock(sku_list: list, verbose=False, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE IM_ITEM
                SET IS_BACK_IN_STOCK = 'N', LST_MAINT_DT = GETDATE()
                WHERE ITEM_NO not in ({','.join([f"'{x}'" for x in sku_list])}) AND
                IS_BACK_IN_STOCK = 'Y'
                """

                response = Database.query(query)

                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Products: {sku_list} updated to BACK IN STOCK.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'Products: {sku_list} not found.')
                else:
                    error = f'Error updating products {sku_list}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            @staticmethod
            def set_inactive(sku, eh=ProcessOutErrorHandler):
                query = f"""
                UPDATE {Table.CP.Item.table} 
                SET STAT = 'V'
                WHERE ITEM_NO = '{sku}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Product {sku} set to inactive status.')
                elif response['code'] == 201:
                    eh.logger.warn(f'Product {sku} not found in {Table.CP.Item.table}.')
                else:
                    error = (
                        f'Error setting product {sku} to inactive status. \n Query: {query}\nResponse: {response}'
                    )
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            @staticmethod
            def update(payload, eh=ProcessInErrorHandler, verbose=False):
                """FOR PRODUCTS_UPDATE WEBHOOK ONLY. Normal updates from shopify_catalog.py use sync()"""
                query = f'UPDATE {Table.CP.Item.table} SET '
                # Item Status
                if 'status' in payload:
                    if payload['status'] == 'active':
                        query += f"{Table.CP.Item.Column.web_visible} = 'Y', "
                    else:
                        query += f"{Table.CP.Item.Column.web_visible} = 'N', "
                # Web Title
                if 'title' in payload:
                    title = Database.sql_scrub(payload['title'])[:80]  # 80 char limit
                    query += f"{Table.CP.Item.Column.web_title} = '{title}', "

                if 'tags' in payload:
                    tags = Database.sql_scrub(payload['tags'])[:80]
                    query += f"{Table.CP.Item.Column.tags} = '{tags}', "
                else:
                    query += f'{Table.CP.Item.Column.tags} = NULL, '

                # SEO Data
                if 'meta_title' in payload:
                    meta_title = Database.sql_scrub(payload['meta_title'])[:80]  # 80 char limit
                    query += f"{Table.CP.Item.Column.meta_title} = '{meta_title}', "
                if 'meta_description' in payload:
                    meta_description = Database.sql_scrub(payload['meta_description'])[:160]  # 160 char limit
                    query += f"{Table.CP.Item.Column.meta_description} = '{meta_description}', "

                # Image Alt Text
                if 'alt_text_1' in payload:
                    alt_text_1 = Database.sql_scrub(payload['alt_text_1'])[:160]  # 160 char limit
                    query += f"{Table.CP.Item.Column.alt_text_1} = '{alt_text_1}', "
                if 'alt_text_2' in payload:
                    alt_text_2 = Database.sql_scrub(payload['alt_text_2'])[:160]  # 160 char limit
                    query += f"{Table.CP.Item.Column.alt_text_2} = '{alt_text_2}', "
                if 'alt_text_3' in payload:
                    alt_text_3 = Database.sql_scrub(payload['alt_text_3'])[:160]  # 160 char limit
                    query += f"{Table.CP.Item.Column.alt_text_3} = '{alt_text_3}', "
                if 'alt_text_4' in payload:
                    alt_text_4 = Database.sql_scrub(payload['alt_text_4'])[:160]  # 160 char limit
                    query += f"{Table.CP.Item.Column.alt_text_4} = '{alt_text_4}', "

                # The following Metafields require an ID to be maintained in the middleware.
                # Check for ID in the respective column. If exists, just update the CP product table.
                # If not, insert the metafield ID into the Middleware and then update the CP product table.

                # Product Status Metafields
                # Work in Progress
                # if payload['item_no'] == '200373':
                #     if payload['is_preorder_item']['id']:
                #         query += f"{Table.CP.Item.Column.is_preorder_item} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.is_preorder_item} = 'N', "

                #     if payload['preorder_message']['id']:
                #         preorder_message = payload['preorder_message']['value'].replace("'", "''")[:160]
                #         query += f"{Table.CP.Item.Column.preorder_message} = '{preorder_message}', "
                #     else:
                #         query += f'{Table.CP.Item.Column.preorder_message} = NULL, '
                #         payload['preorder_message']['id'] = 'NULL'

                #     if payload['preorder_release_date']['id']:
                #         query += f"{Table.CP.Item.Column.preorder_release_date} = '{payload['preorder_release_date']['value']}', "
                #     else:
                #         query += f'{Table.CP.Item.Column.preorder_release_date} = NULL, '
                #         payload['preorder_release_date']['id'] = 'NULL'

                #     if payload['is_new']['id']:
                #         query += f"{Table.CP.Item.Column.is_new} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.is_new} = 'N', "
                #         payload['is_new']['id'] = 'NULL'

                #     if payload['is_back_in_stock']['id']:
                #         query += f"{Table.CP.Item.Column.is_back_in_stock} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.is_back_in_stock} = 'N', "
                #         payload['is_back_in_stock']['id'] = 'NULL'

                #     if payload['is_in_store_only']['id']:
                #         query += f"{Table.CP.Item.Column.in_store_only} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.in_store_only} = 'N', "
                #         payload['is_in_store_only']['id'] = 'NULL'

                #     if payload['is_featured']['id']:
                #         query += f"{Table.CP.Item.Column.featured} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.featured} = 'N', "
                #         payload['is_featured']['id'] = 'NULL'

                #     if payload['is_on_sale']['id']:
                #         query += f"{Table.CP.Item.Column.is_on_sale} = 'Y', "
                #     else:
                #         query += f"{Table.CP.Item.Column.is_on_sale} = 'N', "
                #         payload['is_on_sale']['id'] = 'NULL'

                #     if payload['sale_description']['id']:
                #         sale_description = payload['sale_description']['value'].replace("'", "''")[:160]
                #         query += f"{Table.CP.Item.Column.sale_description} = '{sale_description}', "
                #     else:
                #         query += f'{Table.CP.Item.Column.sale_description} = NULL, '
                #         payload['sale_description']['id'] = 'NULL'

                # # Product Specification Metafields
                # if 'botanical_name' in payload:
                #     query += f"CF_BOTAN_NAM = '{payload['botanical_name']}', "

                # if 'plant_type' in payload:
                #     query += f"CF_PLANT_TYP = '{payload['plant_type']}', "

                # if 'light_requirements' in payload:
                #     query += f"CF_LIGHT_REQ = '{payload['light_requirements']}', "

                # if 'size' in payload:
                #     query += f"CF_SIZE = '{payload['size']}', "

                # if 'features' in payload:
                #     query += f"CF_FEATURES = '{payload['features']}', "

                # if 'bloom_season' in payload:
                #     query += f"CF_BLOOM_SEASON = '{payload['bloom_season']}', "

                # if 'bloom_color' in payload:
                #     query += f"CF_BLOOM_COLOR = '{payload['bloom_color']}', "

                # if 'color' in payload:
                #     query += f"CF_COLOR = '{payload['color']}', "

                if query[-2:] == ', ':
                    query = query[:-2]

                query += f" WHERE ITEM_NO = '{payload['item_no']}'"

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Product {payload["item_no"]} updated in Counterpoint.')
                    # if payload['item_no'] == '200373':
                    # Work in progress
                    # Database.Shopify.Product.Variant.update_metafields(
                    #     sku=payload['item_no'],
                    #     meta_is_preorder=payload['is_preorder_item']['id'],
                    #     meta_preorder_release_date=payload['preorder_release_date']['id'],
                    #     meta_preorder_message=payload['preorder_message']['id'],
                    #     meta_is_new=payload['is_new']['id'],
                    #     meta_is_back_in_stock=payload['is_back_in_stock']['id'],
                    #     meta_in_store_only=payload['is_in_store_only']['id'],
                    #     meta_is_featured=payload['is_featured']['id'],
                    #     meta_is_on_sale=payload['is_on_sale']['id'],
                    #     meta_sale_description=payload['sale_description']['id'],
                    #     verbose=True,
                    #     update_timestamp=False,
                    #     eh=eh,
                    # )

                elif response['code'] == 201:
                    eh.logger.warn(f'Product {payload["item_no"]} not found in Counterpoint.')
                else:
                    error = f'Error updating product {payload["item_no"]} in Counterpoint. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            @staticmethod
            def update_timestamp(item_no, verbose=False, eh=ProcessOutErrorHandler):
                binding_id = Database.CP.Product.get_binding_id(item_no)

                if binding_id:
                    query = f"""
                    UPDATE IM_ITEM
                    SET LST_MAINT_DT = GETDATE()
                    WHERE {Table.CP.Item.Column.binding_id} = '{binding_id}'
                    """
                else:
                    query = f"""
                    UPDATE IM_ITEM
                    SET LST_MAINT_DT = GETDATE()
                    WHERE ITEM_NO = '{item_no}'
                    """

                response = Database.query(query)
                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Timestamp updated for {item_no}.')
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'No rows affected for {item_no}.')
                else:
                    error = f'Error updating timestamp for {item_no}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            @staticmethod
            def update_buffer(item_no, buffer, update_timestamp=True, verbose=False, eh=ProcessOutErrorHandler):
                """Update the stock buffer for an item in Counterpoint."""
                query = f"""
                UPDATE {Table.CP.Item.table}
                SET {Table.CP.Item.Column.buffer} = {buffer}
                WHERE ITEM_NO = '{item_no}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    if verbose:
                        eh.logger.success(f'Buffer updated for {item_no}.')
                    if update_timestamp:
                        Database.CP.Product.update_timestamp(item_no, verbose=verbose, eh=eh)
                elif response['code'] == 201:
                    if verbose:
                        eh.logger.warn(f'No rows affected for {item_no}.')
                else:
                    error = f'Error updating buffer for {item_no}. \n Query: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            class HTMLDescription:
                table = 'EC_ITEM_DESCR'

                def get(item_no):
                    query = f"""
                    SELECT HTML_DESCR 
                    FROM {Table.CP.Item.HTMLDescription.table} 
                    WHERE ITEM_NO = '{item_no}'
                    """
                    return Database.query(query)

                def update(item_no, html_descr, update_timestamp=True, verbose=False, eh=ProcessInErrorHandler):
                    html_descr = Database.sql_scrub(html_descr)
                    query = f"""
                    UPDATE EC_ITEM_DESCR
                    SET HTML_DESCR = '{html_descr}'
                    WHERE ITEM_NO = '{item_no}'
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(f'HTML Description updated for item {item_no}.')
                        if update_timestamp:
                            Database.CP.Product.update_timestamp(item_no, eh=eh, verbose=verbose)
                    elif response['code'] == 201:
                        if verbose:
                            eh.logger.warn(f'No HTML Description found for ITEM_NO: {item_no}.')
                        Database.CP.Product.HTMLDescription.insert(
                            item_no, html_descr, update_timestamp, verbose=verbose, eh=eh
                        )
                    else:
                        error = f'Error updating HTML Description for item {item_no}. \n Query: {query}\nResponse: {response}'
                        eh.error_handler.add_error_v(error=error)
                        raise Exception(error)

                def insert(item_no, html_descr, update_timestamp=True, verbose=False, eh=ProcessInErrorHandler):
                    html_descr = Database.sql_scrub(html_descr)
                    query = f"""
                    INSERT INTO {Table.CP.Item.HTMLDescription.table} (ITEM_NO, HTML_DESCR, LST_MAINT_DT, 
                    LST_MAINT_USR_ID)
                    VALUES ('{item_no}', '{html_descr}', GETDATE(), 'AP')
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(f'INSERT: HTML Description for {item_no}.')
                        if update_timestamp:
                            Database.CP.Product.update_timestamp(item_no, verbose=verbose, eh=eh)
                    else:
                        error = f'Error adding HTML Description for item {item_no}. \nQuery: {query}\nResponse: {response}'
                        eh.error_handler.add_error_v(error=error)
                        raise Exception(error)

            class Media:
                class Video:
                    def get():
                        """Returns a list of SKUs and URLS for all videos in Counterpoint."""
                        query = f"""
                        SELECT ITEM_NO, {Table.CP.Item.Column.videos} 
                        FROM {Table.CP.Item.table}
                        WHERE {Table.CP.Item.Column.videos} IS NOT NULL AND
                        {Table.CP.Item.Column.web_enabled} = 'Y'
                        """
                        response = Database.query(query)
                        all_videos = [[x[0], x[1]] for x in response] if response else []
                        temp_videos = []

                        if all_videos:
                            for entry in all_videos:
                                if ',' in entry[1]:
                                    multi_video_list = entry[1].replace(' ', '').split(',')

                                    for video in multi_video_list:
                                        temp_videos.append([entry[0], video])

                                else:
                                    temp_videos.append(entry)

                        return temp_videos

            class Discount:
                def update(rule):
                    for i in rule.items:
                        # Add Sale Item Flag and Sale Description to Items
                        query = f"""
                        UPDATE {Table.CP.Item.table}
                        SET IS_ON_SALE = 'Y', SALE_DESCR = '{rule.badge_text}', LST_MAINT_DT = GETDATE()
                        WHERE ITEM_NO = '{i}'
                        """
                        # Updating Sale Price, Last Maintenance Date, and Adding to On Sale Category
                        response = Database.query(query)
                        if response['code'] == 200:
                            Database.logger.success(f'Item: {i} Sale Status and Description Added Successfully.')
                        elif response['code'] == 201:
                            Database.logger.warn(f'No Rows Affected for Item: {i}')
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n {response["message"]}, origin="Sale Price Addition")'
                            )

        class Customer:
            def __init__(self, cust_no):
                self.cust = Database.CP.Customer.get(cust_no)
                self.CUST_NO = self.cust['CUST_NO']
                self.NAM = self.cust['NAM']
                self.NAM_UPR = self.cust['NAM_UPR']
                self.FST_NAM = self.cust['FST_NAM']
                self.FST_NAM_UPR = self.cust['FST_NAM_UPR']
                self.LST_NAM = self.cust['LST_NAM']
                self.LST_NAM_UPR = self.cust['LST_NAM_UPR']
                self.SALUTATION = self.cust['SALUTATION']
                self.CUST_TYP = self.cust['CUST_TYP']
                self.ADRS_1 = self.cust['ADRS_1']
                self.ADRS_2 = self.cust['ADRS_2']
                self.ADRS_3 = self.cust['ADRS_3']
                self.CITY = self.cust['CITY']
                self.STATE = self.cust['STATE']
                self.ZIP_COD = self.cust['ZIP_COD']
                self.CNTRY = self.cust['CNTRY']
                self.PHONE_1 = self.cust['PHONE_1']
                self.PHONE_2 = self.cust['PHONE_2']
                self.FAX_1 = self.cust['FAX_1']
                self.FAX_2 = self.cust['FAX_2']
                self.CONTCT_1 = self.cust['CONTCT_1']
                self.CONTCT_2 = self.cust['CONTCT_2']
                self.EMAIL_ADRS_1 = self.cust['EMAIL_ADRS_1']
                self.EMAIL_ADRS_2 = self.cust['EMAIL_ADRS_2']
                self.URL_1 = self.cust['URL_1']
                self.URL_2 = self.cust['URL_2']
                self.PROMPT_NAM_ADRS = self.cust['PROMPT_NAM_ADRS']
                self.SLS_REP = self.cust['SLS_REP']
                self.CATEG_COD = self.cust['CATEG_COD']
                self.SHIP_VIA_COD = self.cust['SHIP_VIA_COD']
                self.SHIP_ZONE_COD = self.cust['SHIP_ZONE_COD']
                self.STR_ID = self.cust['STR_ID']
                self.STMNT_COD = self.cust['STMNT_COD']
                self.TAX_COD = self.cust['TAX_COD']
                self.TERMS_COD = self.cust['TERMS_COD']
                self.COMMNT = self.cust['COMMNT']
                self.TAX_EXEMPT_NO = self.cust['TAX_EXEMPT_NO']
                self.TAX_EXEMPT_DAT = self.cust['TAX_EXEMPT_DAT']
                self.ALLOW_AR_CHRG = self.cust['ALLOW_AR_CHRG']
                self.ALLOW_TKTS = self.cust['ALLOW_TKTS']
                self.NO_CR_LIM = self.cust['NO_CR_LIM']
                self.CR_LIM = self.cust['CR_LIM']
                self.CR_RATE = self.cust['CR_RATE']
                self.NO_MAX_CHK_AMT = self.cust['NO_MAX_CHK_AMT']
                self.MAX_CHK_AMT = self.cust['MAX_CHK_AMT']
                self.UNPSTD_BAL = self.cust['UNPSTD_BAL']
                self.BAL_METH = self.cust['BAL_METH']
                self.AR_ACCT_NO = self.cust['AR_ACCT_NO']
                self.BAL = self.cust['BAL']
                self.ORD_BAL = self.cust['ORD_BAL']
                self.NO_OF_ORDS = self.cust['NO_OF_ORDS']
                self.USE_ORD_SHIP_TO = self.cust['USE_ORD_SHIP_TO']
                self.ALLOW_ORDS = self.cust['ALLOW_ORDS']
                self.LST_AGE_DAT = self.cust['LST_AGE_DAT']
                self.LST_AGE_BAL = self.cust['LST_AGE_BAL']
                self.LST_AGE_BAL_1 = self.cust['LST_AGE_BAL_1']
                self.LST_AGE_BAL_2 = self.cust['LST_AGE_BAL_2']
                self.LST_AGE_BAL_3 = self.cust['LST_AGE_BAL_3']
                self.LST_AGE_BAL_4 = self.cust['LST_AGE_BAL_4']
                self.LST_AGE_BAL_5 = self.cust['LST_AGE_BAL_5']
                self.LST_AGE_BAL_2_5 = self.cust['LST_AGE_BAL_2_5']
                self.LST_AGE_BAL_3_5 = self.cust['LST_AGE_BAL_3_5']
                self.LST_AGE_BAL_4_5 = self.cust['LST_AGE_BAL_4_5']
                self.LST_AGE_BAL_OPN = self.cust['LST_AGE_BAL_OPN']
                self.LST_AGE_FUTR_DOCS = self.cust['LST_AGE_FUTR_DOCS']
                self.LST_AGE_METH = self.cust['LST_AGE_METH']
                self.LST_AGE_AS_OF_DAT = self.cust['LST_AGE_AS_OF_DAT']
                self.LST_AGE_CUTOFF_DAT = self.cust['LST_AGE_CUTOFF_DAT']
                self.LST_AGE_MAX_PRD_1 = self.cust['LST_AGE_MAX_PRD_1']
                self.LST_AGE_MAX_PRD_2 = self.cust['LST_AGE_MAX_PRD_2']
                self.LST_AGE_MAX_PRD_3 = self.cust['LST_AGE_MAX_PRD_3']
                self.LST_AGE_MAX_PRD_4 = self.cust['LST_AGE_MAX_PRD_4']
                self.LST_AGE_NO_OF_PRDS = self.cust['LST_AGE_NO_OF_PRDS']
                self.LST_AGE_EVENT_NO = self.cust['LST_AGE_EVENT_NO']
                self.LST_AGE_NO_CUTOFF = self.cust['LST_AGE_NO_CUTOFF']
                self.LST_AGE_PAST_CUTOFF = self.cust['LST_AGE_PAST_CUTOFF']
                self.LST_AGE_NON_STD = self.cust['LST_AGE_NON_STD']
                self.LST_STMNT_DAT = self.cust['LST_STMNT_DAT']
                self.LST_STMNT_BAL = self.cust['LST_STMNT_BAL']
                self.LST_STMNT_BAL_1 = self.cust['LST_STMNT_BAL_1']
                self.LST_STMNT_BAL_2 = self.cust['LST_STMNT_BAL_2']
                self.LST_STMNT_BAL_3 = self.cust['LST_STMNT_BAL_3']
                self.LST_STMNT_BAL_4 = self.cust['LST_STMNT_BAL_4']
                self.LST_STMNT_BAL_5 = self.cust['LST_STMNT_BAL_5']
                self.LST_STMNT_BAL_2_5 = self.cust['LST_STMNT_BAL_2_5']
                self.LST_STMNT_BAL_3_5 = self.cust['LST_STMNT_BAL_3_5']
                self.LST_STMNT_BAL_4_5 = self.cust['LST_STMNT_BAL_4_5']
                self.LST_STMNT_BAL_OPN = self.cust['LST_STMNT_BAL_OPN']
                self.LST_STMNT_METH = self.cust['LST_STMNT_METH']
                self.LST_STMNT_BEG_DAT = self.cust['LST_STMNT_BEG_DAT']
                self.LST_STMNT_END_DAT = self.cust['LST_STMNT_END_DAT']
                self.LST_STMNT_MAX_PRD_1 = self.cust['LST_STMNT_MAX_PRD_1']
                self.LST_STMNT_MAX_PRD_2 = self.cust['LST_STMNT_MAX_PRD_2']
                self.LST_STMNT_MAX_PRD_3 = self.cust['LST_STMNT_MAX_PRD_3']
                self.LST_STMNT_MAX_PRD_4 = self.cust['LST_STMNT_MAX_PRD_4']
                self.LST_STMNT_NO_OF_PRDS = self.cust['LST_STMNT_NO_OF_PRDS']
                self.LST_STMNT_PAST_CTOFF = self.cust['LST_STMNT_PAST_CTOFF']
                self.FST_SAL_DAT = self.cust['FST_SAL_DAT']
                self.LST_SAL_DAT = self.cust['LST_SAL_DAT']
                self.LST_SAL_AMT = self.cust['LST_SAL_AMT']
                self.LST_PMT_DAT = self.cust['LST_PMT_DAT']
                self.LST_PMT_AMT = self.cust['LST_PMT_AMT']
                self.PROF_ALPHA_1 = self.cust['PROF_ALPHA_1']
                self.PROF_ALPHA_2 = self.cust['PROF_ALPHA_2']
                self.PROF_ALPHA_3 = self.cust['PROF_ALPHA_3']
                self.PROF_ALPHA_4 = self.cust['PROF_ALPHA_4']
                self.PROF_ALPHA_5 = self.cust['PROF_ALPHA_5']
                self.PROF_COD_1 = self.cust['PROF_COD_1']
                self.PROF_COD_2 = self.cust['PROF_COD_2']
                self.PROF_COD_3 = self.cust['PROF_COD_3']
                self.PROF_COD_4 = self.cust['PROF_COD_4']
                self.PROF_COD_5 = self.cust['PROF_COD_5']
                self.PROF_DAT_1 = self.cust['PROF_DAT_1']
                self.PROF_DAT_2 = self.cust['PROF_DAT_2']
                self.PROF_DAT_3 = self.cust['PROF_DAT_3']
                self.PROF_DAT_4 = self.cust['PROF_DAT_4']
                self.PROF_DAT_5 = self.cust['PROF_DAT_5']
                self.PROF_NO_1 = self.cust['PROF_NO_1']
                self.PROF_NO_2 = self.cust['PROF_NO_2']
                self.PROF_NO_3 = self.cust['PROF_NO_3']
                self.PROF_NO_4 = self.cust['PROF_NO_4']
                self.PROF_NO_5 = self.cust['PROF_NO_5']
                self.LST_MAINT_DT = self.cust['LST_MAINT_DT']
                self.LST_MAINT_USR_ID = self.cust['LST_MAINT_USR_ID']
                self.LST_LCK_DT = self.cust['LST_LCK_DT']
                self.ROW_TS = self.cust['ROW_TS']
                self.WRK_STMNT_ACTIV = self.cust['WRK_STMNT_ACTIV']
                self.LWY_BAL = self.cust['LWY_BAL']
                self.NO_OF_LWYS = self.cust['NO_OF_LWYS']
                self.USE_LWY_SHIP_TO = self.cust['USE_LWY_SHIP_TO']
                self.ALLOW_LWYS = self.cust['ALLOW_LWYS']
                self.IS_ECOMM_CUST = self.cust['IS_ECOMM_CUST']
                self.ECOMM_CUST_NO = self.cust['ECOMM_CUST_NO']
                self.ECOMM_AFFIL_COD = self.cust['ECOMM_AFFIL_COD']
                self.DISC_PCT = self.cust['DISC_PCT']
                self.ECOMM_INIT_PWD = self.cust['ECOMM_INIT_PWD']
                self.ECOMM_NXT_PUB_UPDT = self.cust['ECOMM_NXT_PUB_UPDT']
                self.ECOMM_NXT_PUB_FULL = self.cust['ECOMM_NXT_PUB_FULL']
                self.ECOMM_LST_PUB_DT = self.cust['ECOMM_LST_PUB_DT']
                self.ECOMM_LST_PUB_TYP = self.cust['ECOMM_LST_PUB_TYP']
                self.ECOMM_LST_IMP_DT = self.cust['ECOMM_LST_IMP_DT']
                self.ECOMM_CREATED_CUST = self.cust['ECOMM_CREATED_CUST']
                self.ECOMM_LST_ORD_NO = self.cust['ECOMM_LST_ORD_NO']
                self.ECOMM_LST_ORD_DT = self.cust['ECOMM_LST_ORD_DT']
                self.ECOMM_LST_IMP_TYP = self.cust['ECOMM_LST_IMP_TYP']
                self.ECOMM_LST_IMP_EVENT_NO = self.cust['ECOMM_LST_IMP_EVENT_NO']
                self.PROMPT_FOR_CUSTOM_FLDS = self.cust['PROMPT_FOR_CUSTOM_FLDS']
                self.LOY_PGM_COD = self.cust['LOY_PGM_COD']
                self.LOY_PTS_BAL = self.cust['LOY_PTS_BAL']
                self.TOT_LOY_PTS_EARND = self.cust['TOT_LOY_PTS_EARND']
                self.TOT_LOY_PTS_RDM = self.cust['TOT_LOY_PTS_RDM']
                self.TOT_LOY_PTS_ADJ = self.cust['TOT_LOY_PTS_ADJ']
                self.LST_LOY_EARN_TKT_DAT = self.cust['LST_LOY_EARN_TKT_DAT']
                self.LST_LOY_EARN_TKT_TIM = self.cust['LST_LOY_EARN_TKT_TIM']
                self.LST_LOY_PTS_EARN = self.cust['LST_LOY_PTS_EARN']
                self.LST_LOY_EARN_TKT_NO = self.cust['LST_LOY_EARN_TKT_NO']
                self.LST_LOY_RDM_TKT_DAT = self.cust['LST_LOY_RDM_TKT_DAT']
                self.LST_LOY_RDM_TKT_TIM = self.cust['LST_LOY_RDM_TKT_TIM']
                self.LST_LOY_PTS_RDM = self.cust['LST_LOY_PTS_RDM']
                self.LST_LOY_RDM_TKT_NO = self.cust['LST_LOY_RDM_TKT_NO']
                self.LST_LOY_ADJ_DAT = self.cust['LST_LOY_ADJ_DAT']
                self.LST_LOY_PTS_ADJ = self.cust['LST_LOY_PTS_ADJ']
                self.LST_LOY_ADJ_DOC_NO = self.cust['LST_LOY_ADJ_DOC_NO']
                self.LOY_CARD_NO = self.cust['LOY_CARD_NO']
                self.FCH_COD = self.cust['FCH_COD']
                self.LST_FCH_DAT = self.cust['LST_FCH_DAT']
                self.LST_FCH_AMT = self.cust['LST_FCH_AMT']
                self.LST_FCH_PAST_DUE_AMT = self.cust['LST_FCH_PAST_DUE_AMT']
                self.LST_FCH_DOC_NO = self.cust['LST_FCH_DOC_NO']
                self.REQ_PO_NO = self.cust['REQ_PO_NO']
                self.RS_UTC_DT = self.cust['RS_UTC_DT']
                self.CUST_NAM_TYP = self.cust['CUST_NAM_TYP']
                self.CUST_FST_LST_NAM = self.cust['CUST_FST_LST_NAM']
                self.LST_LOY_EARN_TKT_DT = self.cust['LST_LOY_EARN_TKT_DT']
                self.LST_LOY_RDM_TKT_DT = self.cust['LST_LOY_RDM_TKT_DT']
                self.PS_HDR_CUST_FLD_FRM_ID = self.cust['PS_HDR_CUST_FLD_FRM_ID']
                self.EMAIL_STATEMENT = self.cust['EMAIL_STATEMENT']
                self.RS_STAT = self.cust['RS_STAT']
                self.INCLUDE_IN_MARKETING_MAILOUTS = self.cust['INCLUDE_IN_MARKETING_MAILOUTS']
                self.MARKETING_MAILOUT_OPT_IN_DAT = self.cust['MARKETING_MAILOUT_OPT_IN_DAT']
                self.RPT_EMAIL = self.cust['RPT_EMAIL']
                self.MBL_PHONE_1 = self.cust['MBL_PHONE_1']
                self.MBL_PHONE_2 = self.cust['MBL_PHONE_2']
                self.SMS_1_IS_SUB = self.cust['SMS_1_IS_SUB']
                self.SMS_1_OPT_IN_DT = self.cust['SMS_1_OPT_IN_DT']
                self.SMS_1_LST_MAINT_DT = self.cust['SMS_1_LST_MAINT_DT']
                self.SMS_2_IS_SUB = self.cust['SMS_2_IS_SUB']
                self.SMS_2_OPT_IN_DT = self.cust['SMS_2_OPT_IN_DT']
                self.SMS_2_LST_MAINT_DT = self.cust['SMS_2_LST_MAINT_DT']
                self.EMAIL_1_IS_SUB = self.cust['EMAIL_1_IS_SUB']
                self.EMAIL_1_OPT_IN_DT = self.cust['EMAIL_1_OPT_IN_DT']
                self.EMAIL_1_LST_MAINT_DT = self.cust['EMAIL_1_LST_MAINT_DT']
                self.EMAIL_2_IS_SUB = self.cust['EMAIL_2_IS_SUB']
                self.EMAIL_2_OPT_IN_DT = self.cust['EMAIL_2_OPT_IN_DT']
                self.EMAIL_2_LST_MAINT_DT = self.cust['EMAIL_2_LST_MAINT_DT']

                if self.FST_NAM:
                    self.FST_NAM = self.FST_NAM.strip().title()
                if self.LST_NAM:
                    self.LST_NAM = self.LST_NAM.strip().title()
                if self.NAM:
                    self.NAM = self.NAM.strip().title()
                if self.ADRS_1:
                    self.ADRS_1 = self.ADRS_1.strip().title()
                if self.ADRS_2:
                    self.ADRS_2 = self.ADRS_2.strip()
                if self.CITY:
                    self.CITY = self.CITY.strip().title()
                if self.STATE:
                    self.STATE = self.STATE.strip().upper()
                if self.ZIP_COD:
                    self.ZIP_COD = self.ZIP_COD.strip()
                if self.CNTRY:
                    self.CNTRY = self.CNTRY.strip().upper()

            def __str__(self):
                result = ''
                for k, v in self.__dict__.items():
                    if k == 'cust':
                        continue
                    result += f'{k}: {v}\n'
                return result

            @staticmethod
            def add_email(cust_no, email, field='EMAIL_ADRS_1', subscribe=True, eh=ProcessInErrorHandler):
                query = f"""
                UPDATE {Table.CP.Customers.table}
                SET {field} = '{email}'
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Customer {cust_no} email updated to {email}.', origin='add_email')
                    if subscribe:
                        Database.Newsletter.subscribe(email)
                elif response['code'] == 201:
                    eh.logger.warn(f'No rows affected for {cust_no}.', origin='add_email')
                else:
                    eh.error_handler.add_error_v(
                        error=f'Error updating customer {cust_no} email.\n\nQuery: {query}\n\nResponse: {response}',
                        origin='add_email',
                    )
                    raise Exception(response['message'])

            @staticmethod
            def update_first_sale_date(cust_no, first_sale_date):
                if first_sale_date:
                    query = f"""
                    UPDATE {Table.CP.Customers.table}
                    SET FST_SAL_DAT = '{first_sale_date}'
                    WHERE CUST_NO = '{cust_no}'
                    """
                else:
                    query = f"""
                    UPDATE {Table.CP.Customers.table}
                    SET FST_SAL_DAT = NULL
                    WHERE CUST_NO = '{cust_no}'
                    """
                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {cust_no} first sale date updated to {first_sale_date}.')
                elif response['code'] == 201:
                    Database.logger.warn(f'No rows affected for {cust_no}.')
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error updating customer {cust_no} first sale date.\n\nQuery: {query}\n\nResponse: {response}',
                        origin='update_first_sale_date',
                    )
                    raise Exception(response['message'])

            @staticmethod
            def update_last_sale_date(cust_no, last_sale_date, last_sale_amt=None):
                if last_sale_date:
                    query = f"""
                    UPDATE {Table.CP.Customers.table}
                    SET LST_SAL_DAT = '{last_sale_date}', LST_SAL_AMT = {last_sale_amt}
                    WHERE CUST_NO = '{cust_no}'
                    """
                else:
                    query = f"""
                    UPDATE {Table.CP.Customers.table}
                    SET LST_SAL_DAT = NULL, LST_SAL_AMT = NULL
                    WHERE CUST_NO = '{cust_no}'
                    """
                response = Database.query(query)
                if response['code'] == 200:
                    success_message = f'Customer {cust_no} last sale date updated to {last_sale_date}.'
                    if last_sale_amt:
                        success_message += f' Last sale amount updated to {last_sale_amt}.'
                    Database.logger.success(success_message)

                elif response['code'] == 201:
                    Database.logger.warn(f'No rows affected for {cust_no}.')

                else:
                    Database.error_handler.add_error_v(
                        error=f'Error updating customer {cust_no} last sale date.\n\nQuery: {query}\n\nResponse: {response}',
                        origin='update_last_sale_date',
                    )
                    raise Exception(response['message'])

            @staticmethod
            def get(cust_no):
                query = f"""
                SELECT * FROM {Table.CP.Customers.table}
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query, mapped=True)
                if response['code'] == 200 or response['code'] == 201:
                    return response['data'][0]
                else:
                    return {}

            def get_by_email(email):
                query = f"""
                SELECT * FROM {Table.CP.Customers.table}
                WHERE EMAIL_ADRS_1 = '{email}' OR EMAIL_ADRS_2 = '{email}'
                """
                response = Database.query(query, mapped=True)
                if response['code'] == 200:
                    return response['data']
                else:
                    return []

            def find_phone(phone_number: str, eh=ProcessInErrorHandler) -> list[str]:
                """Takes a phone number and returns a list of columns that contain the phone number"""
                query = f"""
                    SELECT {Table.CP.Customers.Column.mobile_phone_1}, {Table.CP.Customers.Column.mobile_phone_2}
                    FROM {Table.CP.Customers.table}
                    WHERE {Table.CP.Customers.Column.mobile_phone_1} = '{phone_number}' 
                    OR {Table.CP.Customers.Column.mobile_phone_2} = '{phone_number}'
                """
                response = Database.query(query=query, mapped=True)
                if response['code'] == 200:
                    result = []
                    data = response['data']
                    for row in data:
                        for column in row:
                            if row[column]:
                                result.append(column)
                    return result
                else:
                    eh.logger.info(f'Phone number {phone_number} not found in Counterpoint.')

            @staticmethod
            def get_all(last_sync=datetime(1970, 1, 1), customer_no=None, customer_list=None):
                if customer_no:
                    customer_filter = f"AND CP.CUST_NO = '{customer_no}'"
                elif customer_list:
                    customer_filter = f'AND CP.CUST_NO IN {tuple(customer_list)}'
                else:
                    customer_filter = ''

                query = f"""
                SELECT CP.CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, MW.LOY_ACCOUNT, ADRS_1, ADRS_2, CITY, STATE, ZIP_COD, CNTRY,
                MW.SHOP_CUST_ID, MW.META_CUST_NO, MW.META_LOY_PTS_BAL, CATEG_COD, MW.META_CATEG, PROF_COD_2, MW.META_BIR_MTH, PROF_COD_3, MW.META_SPS_BIR_MTH, 
                PROF_ALPHA_1, MW.WH_PRC_TIER, {Table.CP.Customers.Column.sms_1_is_subscribed}, {Table.CP.Customers.Column.email_1_is_subscribed}, MW.ID 
                FROM {Table.CP.Customers.table} CP
                FULL OUTER JOIN {Table.Middleware.customers} MW on CP.CUST_NO = MW.cust_no
                WHERE IS_ECOMM_CUST = 'Y' AND CP.LST_MAINT_DT > '{last_sync}' and CUST_NAM_TYP = 'P' {customer_filter}
                """
                response = Database.query(query, mapped=True)
                if response['code'] == 200:
                    return response['data']
                else:
                    return []

            def update(self):
                query = f"""
                UPDATE {Table.CP.Customers.table}
                SET """
                for key, value in self.__dict__.items():
                    if (
                        key
                        in [
                            'cust',
                            'CUST_NO',
                            'LST_MAINT_DT',
                            'ROW_TS',
                            'CUST_FST_LST_NAM',
                            'LST_LOY_EARN_TKT_DT',
                            'LST_LOY_RDM_TKT_DT',
                            'ST_LOY_RDM_TKT_DT',
                        ]
                        or value is None
                    ):
                        continue
                    if isinstance(value, str):
                        value = Database.sql_scrub(value)
                    if isinstance(value, datetime):
                        value = value.strftime('%Y-%m-%d %H:%M:%S')
                    query += f"{key} = '{value}', "

                # Remove trailing comma and space
                if query[-2:] == ', ':
                    query = query[:-2]

                # Update Last Maintenance Date
                query += ', LST_MAINT_DT = GETDATE()'

                # Add WHERE clause
                query += f" WHERE CUST_NO = '{self.CUST_NO}'"

                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {self.CUST_NO} updated.')
                elif response['code'] == 201:
                    Database.logger.warn(f'No rows affected for {self.CUST_NO}.')
                else:
                    error = f'Error updating customer {self.CUST_NO}. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            @staticmethod
            def get_customer_by_phone(phone):
                cp_phone_input = PhoneNumber(phone).to_cp()
                query = f"""
                    SELECT CUST_NO, FST_NAM, LST_NAM, CATEG_COD
                    FROM {Table.CP.Customers.table}
                    WHERE PHONE_1 = '{cp_phone_input}'
                    """
                response = Database.query(query)

                if response is not None:
                    customer_no = response[0][0]
                    first_name = response[0][1]
                    last_name = response[0][2]
                    full_name = first_name + ' ' + last_name
                    category = response[0][3]
                else:
                    # For people with no phone in our database
                    customer_no = 'Unknown'
                    full_name = 'Unknown'
                    category = 'Unknown'

                return customer_no, full_name, category

            @staticmethod
            def lookup_customer_by_email(email_address) -> str:
                """Takes an email address and returns the customer number associated with it."""
                if email_address is None:
                    return
                email_address = email_address.replace("'", "''")
                query = f"""
                SELECT TOP 1 CUST_NO
                FROM AR_CUST 
                WHERE EMAIL_ADRS_1 = '{email_address}' or EMAIL_ADRS_2 = '{email_address}'
                """
                response = Database.query(query)
                if response is not None:
                    return response[0][0]

            def lookup_customer_by_phone(phone_number: str) -> str:
                """Takes a phone number and returns the customer number associated with it."""
                if phone_number is None:
                    return
                phone_number = PhoneNumber(phone_number).to_cp()
                query = f"""
                SELECT TOP 1 CUST_NO
                FROM AR_CUST
                WHERE PHONE_1 = '{phone_number}' or MBL_PHONE_1 = '{phone_number}'
                """
                response = Database.query(query)
                if response is not None:
                    return response[0][0]

            def lookup_customer(email_address=None, phone_number=None) -> str:
                """Looks up a customer by email or phone number and returns customer number."""
                return Database.CP.Customer.lookup_customer_by_email(
                    email_address
                ) or Database.CP.Customer.lookup_customer_by_phone(phone_number)

            def is_customer(email_address, phone_number) -> bool:
                """Checks to see if an email or phone number belongs to a current customer"""
                return (
                    Database.CP.Customer.lookup_customer_by_email(email_address) is not None
                    or Database.CP.Customer.lookup_customer_by_phone(phone_number) is not None
                )

            @staticmethod
            def update_timestamps(customer_list: list = None, customer_no: str = None):
                if not customer_list and not customer_no:
                    return
                if customer_no:
                    customer_list = [customer_no]

                if len(customer_list) == 1:
                    customer_list = f"('{customer_list[0]}')"
                else:
                    customer_list = str(tuple(customer_list))

                query = f"""
                UPDATE {Table.CP.Customers.table}
                SET LST_MAINT_DT = GETDATE()
                WHERE CUST_NO IN {customer_list}"""

                response = Database.query(query)

                if response['code'] == 200:
                    Database.logger.success('Customer timestamps updated.')
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error updating customer timestamps.\n\nQuery: {query}\n\nResponse: {response}',
                        origin='update_customer_timestamps',
                    )
                    raise Exception(response['message'])

            @staticmethod
            def get_cust_no(phone_no):
                phone = PhoneNumber(phone_no).to_cp()
                query = f"""
                SELECT CUST_NO FROM AR_CUST
                WHERE PHONE_1 = '{phone}''
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    return None

            @staticmethod
            def get_category(cust_no):
                query = f"""
                SELECT CATEG_COD FROM AR_CUST
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    return None

            @staticmethod
            def get_loyalty_balance(cust_no: str) -> float:
                query = f"""
                SELECT LOY_PTS_BAL 
                FROM {Table.CP.Customers.table}
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query)
                try:
                    balance = float(response[0][0])
                except:
                    balance = 0

                return float(balance)

            def set_loyalty_balance(cust_no: str, balance: int, eh=ProcessInErrorHandler):
                query = f"""
                UPDATE AR_CUST
                SET LOY_PTS_BAL = {balance}
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query)

                if response['code'] == 200:
                    eh.logger.success('Cust Loyalty points written')
                else:
                    eh.error_handler.add_error_v('Cust Loyalty points could not be written')

            def decrement_orders(cust_no: str, eh=ProcessInErrorHandler):
                # Update the customer order count
                query = f"""
                UPDATE AR_CUST
                SET NO_OF_ORDS = NO_OF_ORDS - 1
                WHERE CUST_NO = '{cust_no}'"""

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success('Updated customer order count')
                elif response['code'] == 201:
                    eh.logger.warn('No rows affected for customer order count')
                else:
                    eh.error_handler.add_error_v('Customer order count could not be updated')
                    eh.error_handler.add_error_v(response['message'])

            @staticmethod
            def get_name(cust_no):
                query = f"""
                SELECT NAM FROM AR_CUST
                WHERE CUST_NO = '{cust_no}'
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    return None

            @staticmethod
            def merge_customer(from_cust_no, to_cust_no, eh=ProcessOutErrorHandler):
                """Merges two customers in Counterpoint. from_cust_no will be merged into to_cust_no."""
                query = f"""
                DECLARE @fromCust varchar(50) = '{from_cust_no}'
                DECLARE @toCust varchar(50) = '{to_cust_no}'
                DECLARE @output varchar(1000)

                UPDATE {Table.CP.Customers.table}
                SET {Table.CP.Customers.Column.is_ecomm_customer} = 'N'
                WHERE {Table.CP.Customers.Column.number} in (@fromCust, @toCust)

                EXEC USP_AR_MERGE_CUST @fromCust, @toCust, @output

                UPDATE {Table.CP.Customers.table}
                SET {Table.CP.Customers.Column.is_ecomm_customer} = 'Y'
                WHERE {Table.CP.Customers.Column.number} = @toCust"""

                response = Database.query(query, mapped=True)
                if response['code'] == 200:
                    eh.logger.success(f'Customer {from_cust_no} merged into {to_cust_no}.')
                else:
                    error = f'Error merging customer {from_cust_no} into {to_cust_no}. \nQuery: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            class Email:
                def find(email_address: str, eh=ProcessInErrorHandler) -> list[str]:
                    """Takes an email address and returns a list of columns that contain the email address"""
                    query = f"""
                        SELECT {Table.CP.Customers.Column.email_1}, {Table.CP.Customers.Column.email_2}
                        FROM {Table.CP.Customers.table}
                        WHERE {Table.CP.Customers.Column.email_1} = '{email_address}' 
                        OR {Table.CP.Customers.Column.email_2} = '{email_address}'
                    """
                    response = Database.query(query=query, mapped=True)
                    if response['code'] == 200:
                        result = []
                        data = response['data']
                        for row in data:
                            for column in row:
                                if row[column]:
                                    result.append(column)
                        return result

                def get_subscriptions(email: str, eh=ProcessInErrorHandler) -> dict:
                    """Returns a dict of email subscriptions."""
                    # Find the columns that contain the email address
                    result = {
                        Table.CP.Customers.Column.email_1_is_subscribed: False,
                        Table.CP.Customers.Column.email_2_is_subscribed: False,
                    }
                    email_locations = Database.CP.Customer.Email.find(email)

                    if not email_locations:
                        return result

                    for location in email_locations:
                        if location == Table.CP.Customers.Column.email_1:
                            email_column = Table.CP.Customers.Column.email_1_is_subscribed

                        elif location == Table.CP.Customers.Column.email_2:
                            email_column = Table.CP.Customers.Column.email_2_is_subscribed

                        else:
                            continue

                        query = f"""
                        SELECT {email_column}
                        FROM {Table.CP.Customers.table}
                        WHERE {location} = '{email}'
                        """
                        response = Database.query(query)

                        try:
                            result[email_column] = response[0][0] == 'Y'
                        except:
                            pass

                    return result

                @staticmethod
                def subscribe(email: str, eh=ProcessOutErrorHandler):
                    if not EmailAddress.is_valid(email=email):
                        return eh.error_handler.add_error_v(f'Invalid email address: {email}')

                    subscriptions = Database.CP.Customer.Email.get_subscriptions(email)
                    for x in subscriptions:
                        if subscriptions[x]:
                            eh.logger.warn(f'{email} already subscribed to {x}.')
                        else:
                            if x == Table.CP.Customers.Column.email_1_is_subscribed:
                                email_column = Table.CP.Customers.Column.email_1

                            elif x == Table.CP.Customers.Column.sms_2_is_subscribed:
                                email_column = Table.CP.Customers.Column.email_2
                            else:
                                continue

                            query = f"""
                            UPDATE {Table.CP.Customers.table}
                            SET {x} = 'Y'
                            WHERE {email_column} = '{email}' and {x} = 'N'
                            """
                            response = Database.query(query)
                            if response['code'] == 200:
                                eh.logger.success(f'Subscribed {email_column}:{email} to {x}')
                            elif response['code'] == 201:
                                pass
                            else:
                                eh.error_handler.add_error_v(f'Error subscribing {email} to marketing emails')

                @staticmethod
                def unsubscribe(email, eh=ProcessInErrorHandler):
                    if not EmailAddress.is_valid(email=email):
                        return eh.error_handler.add_error_v(f'Invalid email address: {email}')

                    subscriptions = Database.CP.Customer.Email.get_subscriptions(email)
                    changes = False
                    if subscriptions:
                        for x in subscriptions:
                            if subscriptions[x]:
                                if x == Table.CP.Customers.Column.email_1_is_subscribed:
                                    email_column = Table.CP.Customers.Column.email_1
                                elif x == Table.CP.Customers.Column.email_2_is_subscribed:
                                    email_column = Table.CP.Customers.Column.email_2
                                else:
                                    continue
                                where = f"{email_column} = '{email}'"

                                query = f"""
                                UPDATE {Table.CP.Customers.table}
                                SET {x} = 'N'
                                WHERE {where} and {x} = 'Y'
                                """
                                response = Database.query(query)
                                if response['code'] == 200:
                                    changes = True
                                    eh.logger.success(f'Unsubscribed {email_column}:{email} from {x}')
                                else:
                                    eh.error_handler.add_error_v(f'Error unsubscribing {email} from SMS')

                    if not changes:
                        eh.logger.warn(f'{email} not found in email subscriptions.')

            class ShippingAddress:
                def get(cust_no):
                    query = f"""
                    SELECT FST_NAM, LST_NAM, ADRS_1, ADRS_2, CITY, STATE, ZIP_COD, CNTRY, PHONE_1
                    FROM {Table.CP.customer_ship_addresses}
                    WHERE CUST_NO = '{cust_no}'"""
                    return Database.query(query)

                def insert(customer):
                    columns = ', '.join([x for x in customer.keys()])
                    values = ', '.join([f"'{x}'" for x in customer.values()])
                    query = f"""
                    INSERT INTO {Table.CP.customer_ship_addresses} ({columns}) VALUES({values})
                    """
                    response = Database.query(query)
                    return response

                @staticmethod
                def merge(from_cust_no, to_cust_no, eh=ProcessOutErrorHandler):
                    # Check if from customer has a shipping address
                    query = f"""
                    SELECT COUNT(*) FROM {Table.CP.customer_ship_addresses}
                    WHERE CUST_NO = '{from_cust_no}'
                    """
                    response = Database.query(query)
                    if response[0][0] == 0:
                        eh.logger.warn(f'No shipping addresses found for {from_cust_no}.')
                        return
                    # Check if to customer already has a shipping address
                    query = f"""
                    SELECT COUNT(*) FROM {Table.CP.customer_ship_addresses}
                    WHERE CUST_NO = '{to_cust_no}'
                    """
                    response = Database.query(query)
                    if response[0][0] == 0:
                        # Update shipping address to new customer number. Simple migration of CUST_ID.
                        query = f"""
                        UPDATE {Table.CP.customer_ship_addresses}
                        SET CUST_NO = '{to_cust_no}'
                        WHERE CUST_NO = '{from_cust_no}'
                        """
                        response = Database.query(query)
                        if response['code'] == 200:
                            eh.logger.success(f'Shipping addresses merged from {from_cust_no} to {to_cust_no}.')
                        else:
                            error = f'Error merging shipping addresses from {from_cust_no} to {to_cust_no}. \nQuery: {query}\nResponse: {response}'
                            eh.error_handler.add_error_v(error=error)
                            raise Exception(error)
                    else:
                        # Get a list of shipping addresses associated with the from_cust_no
                        query = f"""
                        SELECT SHIP_ADRS_ID FROM {Table.CP.customer_ship_addresses}
                        WHERE CUST_NO = '{from_cust_no}'
                        """
                        response = Database.query(query)
                        if response:
                            # Generate a random number for the new address ID
                            new_id_int = random.randint(1000, 9999)
                            count = 1
                            for address in response:
                                old_id = address[0]
                                new_id = f'{new_id_int}-{count}'
                                # Update the shipping address to the new customer number
                                query = f"""
                                UPDATE {Table.CP.customer_ship_addresses}
                                SET CUST_NO = '{to_cust_no}', SHIP_ADRS_ID = '{new_id}'
                                WHERE CUST_NO = '{from_cust_no}' AND SHIP_ADRS_ID = '{old_id}'
                                """
                                response = Database.query(query)
                                if response['code'] == 200:
                                    eh.logger.success(
                                        f'Shipping address {old_id} merged from {from_cust_no} to {to_cust_no}.'
                                    )
                                else:
                                    error = f'Error merging shipping address {old_id} from {from_cust_no} to {to_cust_no}. \nQuery: {query}\nResponse: {response}'
                                    eh.error_handler.add_error_v(error=error)
                                    raise Exception(error)
                                count += 1

        class Promotion:
            def get(group_code=None, ids_only=False):
                if group_code:
                    promotions = [group_code]
                else:
                    # Get list of promotions from IM_PRC_GRP
                    response = Database.query("SELECT GRP_COD FROM IM_PRC_GRP WHERE GRP_TYP = 'P'")
                    promotions = [x[0] for x in response] if response else []
                    if ids_only:
                        return promotions

                if promotions:
                    # Get promotion details from IM_PRC_GRP and IM_PRC_GRP_RUL
                    result = []
                    for promo in promotions:
                        # Get promotion details
                        query = f"""
                        SELECT GRP.GRP_TYP, GRP.GRP_COD, GRP.GRP_SEQ_NO, GRP.DESCR, GRP.CUST_FILT, GRP.NO_BEG_DAT, 
                        GRP.BEG_DAT, GRP.BEG_TIM_FLG, GRP.NO_END_DAT, GRP.END_DAT, GRP.END_TIM_FLG, GRP.LST_MAINT_DT, 
                        GRP.ENABLED, GRP.MIX_MATCH_COD                          
                        FROM IM_PRC_GRP GRP
                        WHERE GRP.GRP_COD = '{promo}' and GRP.GRP_TYP = 'P'
                        """
                        response = Database.query(query=query, mapped=True)
                        if response['code'] == 200:
                            promotion = response['data'][0] if response else None
                        else:
                            promotion = None

                        if promotion:
                            result.append(promotion)
                    return result

            class PriceRule:
                def get(group_code):
                    query = f"""
                    SELECT RUL.GRP_TYP, RUL.GRP_COD, RUL.RUL_SEQ_NO, RUL.DESCR, RUL.CUST_FILT, ITEM_FILT, 
                    SAL_FILT, IS_CUSTOM, USE_BOGO_TWOFER, REQ_FULL_GRP_FOR_BOGO_TWOFER,
                    GRP.ENABLED
                    FROM IM_PRC_RUL RUL
					FULL OUTER JOIN IM_PRC_GRP GRP on GRP.GRP_COD = RUL.GRP_COD
                    WHERE RUL.GRP_COD = '{group_code}'
                    """
                    response = Database.query(query, mapped=True)
                    if response['code'] == 200:
                        result = [rule for rule in response['data']] if response else []
                        if result:
                            for x in result:
                                x['ID'] = None
                                x['SHOP_ID'] = None
                                x['MW_ENABLED'] = None
                                # Check Middleware for Shopify ID, etc.
                                query = f"""
                                SELECT ID, SHOP_ID, ENABLED
                                FROM {Table.Middleware.promotions}
                                WHERE GRP_COD = '{group_code}' and RUL_SEQ_NO = '{x['RUL_SEQ_NO']}'
                                """
                                response = Database.query(query)
                                if response:
                                    x['ID'] = response[0][0]
                                    x['SHOP_ID'] = response[0][1]
                                    x['MW_ENABLED'] = response[0][2]

                        return result

        class Discount:
            """Discounts are stored in the PS_DISC_COD table in CounterPoint. These codes are
            used to apply discounts to orders. Discounts can be applied to the entire order or to
            individual items. Discounts can be a fixed amount, a percentage, or a prompted amount or percentage."""

            def get_disc_cod_from_shop_id(shop_id):
                query = f"""
                SELECT DISC_ID FROM {Table.Middleware.discounts} WHERE SHOP_ID = {shop_id}
                """
                response = Database.query(query)
                if response:
                    return response[0][0]
                else:
                    return None

            def has_coupon(code):
                query = f"""
                SELECT COUNT(*) FROM PS_DISC_COD WHERE DISC_COD = '{code}'
                """
                try:
                    response = Database.query(query)
                    if response is not None:
                        return int(response[0][0]) > 0

                    return False
                except Exception as e:
                    Database.error_handler.add_error_v(
                        error=f'CP Coupon Check Error: {e}\nCode: {code}',
                        origin='Database.CP.Discount.cp_has_coupon',
                    )
                    return False

            def create(code, description, amount, min_purchase, coupon_type='A', apply_to='H', store='B'):
                """Will create a coupon code in SQL Database.
                Code is the coupon code, Description is the description of the coupon, Coupon Type is the type of
                coupon, Coupon Types: Amount ('A'), Prompted Amount ('B'), Percentage ('P'), Prompted Percent ('Q')
                Amount is the amount of the coupon, Min Purchase is the minimum purchase amount for the coupon to
                be valid. Apply to is either 'H' for Document or 'L' for Line ('H' is default), Store is either 'B'
                for Both instore and online or 'I' for in-store only ('B' is default)"""

                top_id_query = 'SELECT MAX(DISC_ID) FROM PS_DISC_COD'
                response = Database.query(top_id_query)
                top_id = None
                if response is not None:
                    top_id = response[0][0]
                    top_id += 1
                    query = f"""
                    INSERT INTO PS_DISC_COD(DISC_ID, DISC_COD, DISC_DESCR, DISC_TYP, DISC_AMT, APPLY_TO, 
                    MIN_DISCNTBL_AMT, DISC_VAL_FOR)
                    VALUES ('{top_id}', '{code}', '{description}', '{coupon_type}', '{amount}', '{apply_to}', 
                    '{min_purchase}', '{store}')
                    """
                    try:
                        Database.query(query)
                    except Exception as e:
                        Database.error_handler.add_error_v(
                            error=f'CP Coupon Insertion Error: {e}', origin='Database.CP.Discount.create'
                        )
                    else:
                        Database.logger.success('CP Coupon Insertion Success!')

                else:
                    Database.logger.info('Error: Could not create coupon')

                return top_id

            def deactivate(shop_id: int = None, discount_code: str = None):
                """Deactivates a coupon in CounterPoint. Since there is no way to deactivate a discount code,
                the minimum discountable amount is set to $100,000. This will effectively deactivate the coupon."""
                if not shop_id and not discount_code:
                    Database.error_handler.add_error_v(
                        'No shop_id or discount_code provided', origin='Database.CP.Discount.deactivate'
                    )
                    return
                if shop_id:
                    where = f'WHERE SHOP_ID = {shop_id}'

                elif discount_code:
                    where = f"WHERE DISC_COD = '{discount_code}'"

                query = f"""
                SELECT DISC_ID FROM {Table.Middleware.discounts_view} {where}
                """
                try:
                    response = Database.query(query)
                    for row in response:
                        disc_id = row[0]
                        query = f"""
                        UPDATE {Table.CP.discounts}
                        SET MIN_DISCNTBL_AMT = 100000
                        WHERE DISC_ID = '{disc_id}'
                        """
                        response = Database.query(query)
                        if response['code'] == 200:
                            Database.logger.success('Shopify Coupon Deactivated Successfully!')
                        else:
                            Database.error_handler.add_error_v(
                                'Error deactivating coupon', origin='Database.CP.Discount.deactivate'
                            )
                except Exception as e:
                    Database.error_handler.add_error_v(
                        f'Error deactivating coupon: {e}', origin='Database.CP.Discount.deactivate'
                    )

            def delete(discount_code=None, shop_id=None):
                """Deletes a discount code from CounterPoint."""
                if not discount_code and not shop_id:
                    Database.error_handler.add_error_v(
                        'No discount_code or shop_id provided', origin='Database.CP.Discount.delete'
                    )
                    return

                if shop_id:
                    discount_code = Database.CP.Discount.get_disc_cod_from_shop_id(shop_id)

                if discount_code:
                    query = f"""
                    DELETE FROM {Table.CP.discounts} WHERE DISC_ID = '{discount_code}'
                    """
                    try:
                        response = Database.query(query)

                        if response['code'] == 200:
                            Database.logger.success(f'Deleted Coupon: {discount_code}')
                            return True
                        elif response['code'] == 201:
                            Database.error_handler.add_error_v(
                                f'Could not find coupon in CounterPoint: {discount_code}'
                            )
                            return False
                        else:
                            Database.error_handler.add_error_v(
                                error=f'CP Coupon Deletion Error: {response}', origin='Database.CP.Discount.delete'
                            )
                            return False
                    except Exception as e:
                        Database.error_handler.add_error_v(
                            error=f'CP Coupon Deletion Error: {e}', origin='Database.CP.Discount.delete'
                        )

            def write_discount(
                doc_id: int,
                disc_seq_no: int,
                disc_amt: float,
                disc_id: str,
                apply_to: str,
                disc_type: str,
                disc_pct: float,
                disc_amt_shipped,
                lin_seq_no: int = None,
                eh=ProcessInErrorHandler,
            ):
                """Write an order discount to the PS_DOC_DISC table."""
                query = f"""
                INSERT INTO PS_DOC_DISC
                (DOC_ID, DISC_SEQ_NO, LIN_SEQ_NO, DISC_ID, APPLY_TO, DISC_TYP, 
                DISC_AMT, DISC_PCT, DISC_AMT_SHIPPED)
                
                VALUES
                
                ('{doc_id}', {disc_seq_no}, {lin_seq_no or "NULL"}, {disc_id}, '{apply_to}', '{disc_type}', 
                {disc_amt}, {disc_pct}, {disc_amt_shipped})
                """

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Discount {disc_seq_no} created')
                else:
                    eh.error_handler.add_error_v(f'Discount {disc_seq_no} could not be created')

    class Shopify:
        def rebuild_tables(self):
            def create_tables():
                tables = {
                    'categories': f"""
                                            CREATE TABLE {Table.Middleware.collections} (
                                            CATEG_ID int IDENTITY(1,1) PRIMARY KEY,
                                            COLLECTION_ID bigint,
                                            MENU_ID bigint,
                                            CP_CATEG_ID bigint NOT NULL,
                                            CP_PARENT_ID bigint,
                                            CATEG_NAME nvarchar(255) NOT NULL,
                                            SORT_ORDER int,
                                            DESCRIPTION text,
                                            IS_VISIBLE BIT NOT NULL DEFAULT(1),
                                            IMG_SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'products': f"""
                                            CREATE TABLE {Table.Middleware.products} (                                        
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50) NOT NULL,
                                            BINDING_ID varchar(10),
                                            IS_PARENT BIT,
                                            PRODUCT_ID bigint NOT NULL,
                                            VARIANT_ID bigint,
                                            INVENTORY_ID bigint,
                                            VARIANT_NAME nvarchar(255),
                                            OPTION_ID bigint,
                                            OPTION_VALUE_ID bigint,
                                            CATEG_ID varchar(255),
                                            CF_BOTAN_NAM bigint, 
                                            CF_CLIM_ZON bigint, 
                                            CF_PLANT_TYP bigint, 
                                            CF_TYP bigint, 
                                            CF_HEIGHT bigint, 
                                            CF_WIDTH bigint, 
                                            CF_SUN_EXP bigint, 
                                            CF_BLOOM_TIM bigint,
                                            CF_FLOW_COL bigint,
                                            CF_POLLIN bigint, 
                                            CF_GROWTH_RT bigint, 
                                            CF_DEER_RES bigint, 
                                            CF_SOIL_TYP bigint,
                                            CF_COLOR bigint,
                                            CF_SIZE bigint,
                                            CF_BLOOM_SEAS bigint,
                                            CF_LIGHT_REQ bigint,
                                            CF_FEATURES bigint,
                                            CF_CLIM_ZON bigint,
                                            CF_CLIM_ZON_LST bigint,
                                            CF_IS_PREORDER bigint,
                                            CF_PREORDER_DT bigint,
                                            CF_PREORDER_MSG bigint,
                                            CF_IS_FEATURED bigint,
                                            CF_IS_IN_STORE_ONLY bigint,
                                            CF_IS_ON_SALE bigint, 
                                            CF_SALE_DESCR bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'images': f"""
                                            CREATE TABLE {Table.Middleware.images} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            IMAGE_NAME nvarchar(255) NOT NULL,
                                            ITEM_NO varchar(50),
                                            FILE_PATH nvarchar(255) NOT NULL,
                                            PRODUCT_ID bigint,
                                            IMAGE_ID bigint,
                                            THUMBNAIL BIT DEFAULT(0),
                                            IMAGE_NUMBER int DEFAULT(1),
                                            SORT_ORDER int,
                                            IS_BINDING_IMAGE BIT NOT NULL,
                                            BINDING_ID varchar(50),
                                            IS_VARIANT_IMAGE BIT DEFAULT(0),
                                            DESCR nvarchar(255),
                                            SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'videos': f"""
                                            CREATE TABLE {Table.Middleware.images} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            ITEM_NO varchar(50),
                                            URL nvarchar(255),
                                            VIDEO_NAME nvarchar(255),
                                            FILE_PATH nvarchar(255),
                                            PRODUCT_ID bigint,
                                            VIDEO_ID bigint,
                                            SORT_ORDER int,
                                            BINDING_ID varchar(50),
                                            DESCR nvarchar(255),
                                            SIZE int,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'customers': f"""
                                            CREATE TABLE {Table.Middleware.customers} (
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            CUST_NO varchar(50) NOT NULL,
                                            SHOP_CUST_ID bigint,
                                            META_CUST_NO bigint,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );
                                            """,
                    'orders': f"""
                                            CREATE TABLE {Table.Middleware.orders} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            ORDER_NO int NOT NULL,
                                            DOC_ID bigint,
                                            STATUS bit DEFAULT(0)
                                            )""",
                    'draft_orders': f"""
                                            CREATE TABLE {Table.Middleware.draft_orders} (  
                                            ID int IDENTITY(1,1) primary key,
                                            DOC_ID bigint not NULL,
                                            DRAFT_ID varchar(64) not NULL,
                                            CREATED_DT DATETIME DEFAULT(current_timestamp)
                                            );""",
                    'gift': f"""
                                            CREATE TABLE {Table.Middleware.gift_certificates} (
                                            ID int IDENTITY(1, 1) PRIMARY KEY,
                                            GFC_NO varchar(30) NOT NULL,
                                            BC_GFC_ID int NOT NULL,
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            )""",
                    'promo': f""" 
                                            CREATE TABLE {Table.Middleware.promotions}(
                                            ID int IDENTITY(1,1) PRIMARY KEY,
                                            GRP_COD nvarchar(50) NOT NULL,
                                            RUL_SEQ_NO int,
                                            SHOP_ID bigint PRIMARY KEY,
                                            ENABLED bit DEFAULT(0),
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)
                                            );""",
                    'promo_line_bogo': f"""
                                            CREATE TABLE {Table.Middleware.promotion_lines_bogo}(
                                            SHOP_ID bigint FOREIGN KEY REFERENCES {Table.Middleware.promotions}(SHOP_ID),
                                            ITEM_NO nvarchar(50),
                                            LST_MAINT_DT datetime NOT NULL DEFAULT(current_timestamp)

                                            );""",
                    'promo_line_fixed': f"""
                                            CREATE TABLE {Table.Middleware.promotion_lines_fixed}(
                                            GRP_COD nvarchar(50) NOT NULL,
                                            RUL_SEQ_NO int,
                                            ITEM_NO nvarchar(50)
                                            );""",
                    'discount': f"""
                                            CREATE TABLE {Table.Middleware.discounts} (
                                            ID int IDENTITY(1,1) primary key,
                                            SHOP_ID bigint,
                                            DISC_ID bigint,
                                            CREATED_DT DATETIME DEFAULT(current_timestamp)
                                            );""",
                    'metafields': f""" 
                                            CREATE TABLE {Table.Middleware.metafields}(
                                            META_ID bigint NOT NULL, 
                                            NAME varchar(50) NOT NULL, 
                                            DESCR varchar(255),
                                            NAME_SPACE varchar(50), 
                                            META_KEY varchar(50), 
                                            TYPE varchar(50), 
                                            PINNED_POS bit, 
                                            OWNER_TYPE varchar(50),
                                            VALID_NAME varchar(50),
                                            VALID_VALUE varchar(255),
                                            LST_MAINT_DT DATETIME DEFAULT(current_timestamp))""",
                }

                for table in tables:
                    Database.query(tables[table])

            # Drop Tables
            def drop_tables():
                tables = [
                    creds.bc_customer_table,
                    creds.bc_image_table,
                    creds.bc_product_table,
                    creds.bc_brands_table,
                    creds.bc_category_table,
                    creds.bc_gift_cert_table,
                    creds.bc_order_table,
                ]

                def drop_table(table_name):
                    Database.query(f'DROP TABLE {table_name}')

                for table in tables:
                    drop_table(table)

            # Recreate Tables
            drop_tables()
            create_tables()

        class Customer:
            def get(customer_no=None, column=None):
                if column:
                    query_column = column
                else:
                    query_column = '*'

                if customer_no:
                    query = f"""
                            SELECT {query_column} FROM {Table.Middleware.customers}
                            WHERE CUST_NO = '{customer_no}'
                            """
                else:
                    query = f"""
                            SELECT {query_column} FROM {Table.Middleware.customers}
                            """
                return Database.query(query)

            def get_id(cp_cust_no):
                query = f"""
                        SELECT SHOP_CUST_ID FROM {Table.Middleware.customers}
                        WHERE CUST_NO = '{cp_cust_no}'
                        """
                response = Database.query(query)
                return response[0][0] if response else None

            def exists(shopify_cust_no):
                query = f"""
                        SELECT * FROM {Table.Middleware.customers}
                        WHERE SHOP_CUST_ID = {shopify_cust_no}
                        """
                return Database.query(query)

            def insert(
                shopify_cust_no: int,
                cp_cust_no: str = None,
                store_credit_id: int = None,
                meta_cust_no_id: int = None,
                meta_loyalty_point_id: int = None,
                meta_category_id: int = None,
                meta_birth_month_id: int = None,
                meta_spouse_birth_month_id: int = None,
                meta_wholesale_price_tier_id: int = None,
                eh=ProcessOutErrorHandler,
            ):
                """Inserts a customer into the Middleware database."""
                query = f"""
                        INSERT INTO {Table.Middleware.customers} (CUST_NO, SHOP_CUST_ID, 
                        LOY_ACCOUNT, META_CUST_NO, META_LOY_PTS_BAL, META_CATEG, META_BIR_MTH, 
                        META_SPS_BIR_MTH, WH_PRC_TIER)
                        
                        VALUES ({f"'{cp_cust_no}'" if cp_cust_no else 'NULL'}, '{shopify_cust_no}', 
                        {store_credit_id if store_credit_id else "NULL"}, 
                        {meta_cust_no_id if meta_cust_no_id else "NULL"}, 
                        {meta_loyalty_point_id if meta_loyalty_point_id else "NULL"},
                        {meta_category_id if meta_category_id else "NULL"}, 
                        {meta_birth_month_id if meta_birth_month_id else "NULL"}, 
                        {meta_spouse_birth_month_id if meta_spouse_birth_month_id else "NULL"}, 
                        {meta_wholesale_price_tier_id if meta_wholesale_price_tier_id else "NULL"})
                        """
                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Customer {cp_cust_no} added to Middleware.')
                else:
                    error = (
                        f'Error adding customer {cp_cust_no} to Middleware. \nQuery: {query}\nResponse: {response}'
                    )
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(
                shopify_cust_no: int,
                cp_cust_no: str = None,
                store_credit_id: int = None,
                meta_cust_no_id: int = None,
                meta_loyalty_point_id: int = None,
                meta_category_id: int = None,
                meta_birth_month_id: int = None,
                meta_spouse_birth_month_id: int = None,
                meta_wholesale_price_tier_id: int = None,
                eh=ProcessOutErrorHandler,
            ):
                """Updates a customer in the Middleware database."""
                if cp_cust_no:
                    where = f"CUST_NO = '{cp_cust_no}'"
                    cust_value = ''
                else:
                    # If no CP Customer Number is provided, we will use the Shopify Customer Number
                    where = f'SHOP_CUST_ID = {shopify_cust_no}'
                    cust_value = f"""CUST_NO = {f"'{cp_cust_no}'" if cp_cust_no else "NULL"}, """

                query = f"""
                        UPDATE {Table.Middleware.customers}
                        SET SHOP_CUST_ID = {shopify_cust_no},
                        {cust_value}
                        LOY_ACCOUNT = {store_credit_id if store_credit_id else "NULL"},
                        META_CUST_NO = {meta_cust_no_id if meta_cust_no_id else "NULL"},
                        META_LOY_PTS_BAL = {meta_loyalty_point_id if meta_loyalty_point_id else "NULL"},
                        META_CATEG = {meta_category_id if meta_category_id else "NULL"},
                        META_BIR_MTH = {meta_birth_month_id if meta_birth_month_id else "NULL"},
                        META_SPS_BIR_MTH = {meta_spouse_birth_month_id if meta_spouse_birth_month_id else "NULL"},
                        WH_PRC_TIER = {meta_wholesale_price_tier_id if meta_wholesale_price_tier_id else "NULL"},
                        LST_MAINT_DT = GETDATE()
                        WHERE {where}
                        """

                response = Database.query(query)
                if response['code'] == 200:
                    eh.logger.success(f'Customer {cp_cust_no} updated in Middleware.')
                elif response['code'] == 201:
                    eh.logger.warn(f'Customer {cp_cust_no} not found in Middleware.')
                else:
                    error = f'Error updating customer {cp_cust_no} in Middleware. \nQuery: {query}\nResponse: {response}'
                    eh.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def sync(customer):
                if not customer.cp_cust_no and customer.mw_id:
                    Database.Shopify.Customer.delete(customer)
                else:
                    if customer.mw_id:
                        Database.Shopify.Customer.update(
                            cp_cust_no=customer.cp_cust_no,
                            shopify_cust_no=customer.shopify_cust_no,
                            store_credit_id=customer.store_credit_id,
                            meta_cust_no_id=customer.meta_cust_no_id,
                            meta_loyalty_point_id=customer.meta_loyalty_point_id,
                            meta_category_id=customer.meta_category_id,
                            meta_birth_month_id=customer.meta_birth_month_id,
                            meta_spouse_birth_month_id=customer.meta_spouse_birth_month_id,
                            meta_wholesale_price_tier_id=customer.meta_wholesale_price_tier_id,
                        )
                    else:
                        Database.Shopify.Customer.insert(
                            cp_cust_no=customer.cp_cust_no,
                            shopify_cust_no=customer.shopify_cust_no,
                            store_credit_id=customer.store_credit_id,
                            meta_cust_no_id=customer.meta_cust_no_id,
                            meta_loyalty_point_id=customer.meta_loyalty_point_id,
                            meta_category_id=customer.meta_category_id,
                            meta_birth_month_id=customer.meta_birth_month_id,
                            meta_spouse_birth_month_id=customer.meta_spouse_birth_month_id,
                            meta_wholesale_price_tier_id=customer.meta_wholesale_price_tier_id,
                        )

            def delete(shopify_cust_no):
                if not shopify_cust_no:
                    return
                query = f'DELETE FROM {Table.Middleware.customers} WHERE SHOP_CUST_ID = {shopify_cust_no}'
                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Customer {shopify_cust_no} deleted from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Customer {shopify_cust_no} not found in Middleware.')
                else:
                    error = f'Error deleting customer {shopify_cust_no} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            class Metafield:
                def delete(cp_cust_no: str = None, shopify_cust_no: int = None, column=None):
                    """Delete metafield(s) for customer using either CP or Shopify customer number.
                    If column is provided, only that column will be deleted.
                    Otherwise, all metafields will be deleted."""
                    if not cp_cust_no and not shopify_cust_no:
                        return
                    query = f"""
                    UPDATE {Table.Middleware.customers}
                    SET 
                    """
                    if column:
                        query += f' {column} = NULL'
                    else:
                        # Delete All Metafield Column Data for Customer
                        cust_meta_columns = creds.Shopify.Metafield.Customer.__dict__.keys()
                        for column in cust_meta_columns:
                            if not column.startswith('__'):
                                key = creds.Shopify.Metafield.Customer.__dict__[column]
                                query += f' {key} = NULL,'
                        # Remove trailing comma
                        if query[-1] == ',':
                            query = query[:-1]

                    if cp_cust_no:
                        query += f" WHERE CUST_NO = '{cp_cust_no}'"
                    elif shopify_cust_no:
                        query += f' WHERE SHOP_CUST_ID = {shopify_cust_no}'

                    response = Database.query(query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'Metafields for CUST: {cp_cust_no or shopify_cust_no} deleted from Middleware.'
                        )
                    elif response['code'] == 201:
                        Database.logger.warn(
                            f'Metafields for CUST: {cp_cust_no or shopify_cust_no} not found in Middleware. \nQuery:\n{query}'
                        )
                    else:
                        error = f'Error deleting metafields for CUST: {cp_cust_no or shopify_cust_no} from Middleware.\nQuery:\n{query}\nResponse: {response}'
                        Database.error_handler.add_error_v(error=error)
                        raise Exception(error)

        class Order:
            pass

        class Collection:
            def get_cp_categ_id(collection_id):
                query = f"""
                        SELECT CP_CATEG_ID FROM {Table.Middleware.collections}
                        WHERE COLLECTION_ID = {collection_id}
                        """
                response = Database.query(query)
                try:
                    return response[0][0]
                except:
                    return None

            def insert(category):
                query = f"""
                INSERT INTO {Table.Middleware.collections}(COLLECTION_ID, MENU_ID, CP_CATEG_ID, CP_PARENT_ID, CATEG_NAME, 
                SORT_ORDER, DESCRIPTION, IS_VISIBLE, IMG_SIZE, LST_MAINT_DT)
                VALUES({category.collection_id if category.collection_id else 'NULL'}, 
                {category.menu_id if category.menu_id else 'NULL'}, {category.cp_categ_id}, 
                {category.cp_parent_id}, '{category.name}', {category.sort_order}, 
                '{Database.sql_scrub(category.description)}', {1 if category.is_visible else 0}, 
                {category.image_size if category.image_size else 'NULL'},
                '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}')
                """
                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} added to Middleware.')
                else:
                    error = f'Error adding category {category.name} to Middleware. \nQuery: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def update(category):
                query = f"""
                UPDATE {Table.Middleware.collections}
                SET COLLECTION_ID = {category.collection_id if category.collection_id else 'NULL'}, 
                MENU_ID = {category.menu_id if category.menu_id else 'NULL'},
                CP_PARENT_ID = {category.cp_parent_id}, CATEG_NAME = '{category.name}',
                SORT_ORDER = {category.sort_order}, DESCRIPTION = '{category.description}', 
                IS_VISIBLE = {1 if category.is_visible else 0},
                IMG_SIZE = {category.image_size if category.image_size else 'NULL'},
                LST_MAINT_DT = '{category.lst_maint_dt:%Y-%m-%d %H:%M:%S}'
                WHERE CP_CATEG_ID = {category.cp_categ_id}
                """
                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {category.name} updated in Middleware.')
                else:
                    error = f'Error updating category {category.name} in Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def delete(collection_id=None, cp_categ_id=None):
                if collection_id is not None:
                    cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)

                if cp_categ_id is None:
                    raise Exception('No CP_CATEG_ID provided for deletion.')

                query = f"""
                        DELETE FROM {Table.Middleware.collections}
                        WHERE CP_CATEG_ID = {cp_categ_id}
                        """
                response = Database.query(query)
                if response['code'] == 200:
                    Database.logger.success(f'Category {cp_categ_id} deleted from Middleware.')
                else:
                    error = f'Error deleting category {cp_categ_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

            def backfill_html_description(collection_id, description):
                cp_categ_id = Database.Shopify.Collection.get_cp_categ_id(collection_id)
                query = f"""
                        UPDATE EC_CATEG
                        SET HTML_DESCR = '{description}'
                        WHERE CATEG_ID = '{cp_categ_id}'
                        """
                response = Database.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

        class Product:
            def exists(sku):
                query = f"""
                        SELECT * FROM {Table.Middleware.products}
                        WHERE ITEM_NO = '{sku}'
                        """
                return Database.query(query) is not None

            def get_by_category(cp_category=None, cp_subcategory=None):
                query = f"""SELECT ITEM_NO FROM {Table.CP.Item.table} 
                WHERE {Table.CP.Item.Column.web_enabled} = 'Y' AND 
                ({Table.CP.Item.Column.binding_id} IS NULL OR
                {Table.CP.Item.Column.is_parent} = 'Y') AND 
                """
                if cp_category and cp_subcategory:
                    query += f" CATEG_COD = '{cp_category}' AND SUBCAT_COD = '{cp_subcategory}'"
                else:
                    if cp_category:
                        query += f" CATEG_COD = '{cp_category}'"
                    if cp_subcategory:
                        query += f" SUBCAT_COD = '{cp_subcategory}'"

                response = Database.query(query)
                try:
                    sku_list = [x[0] for x in response] if response else None
                except:
                    sku_list = None

                if sku_list:
                    product_list = []
                    for sku in sku_list:
                        product_list.append(Database.Shopify.Product.get_id(item_no=sku))
                    return product_list

            def get_id(item_no=None, binding_id=None, image_id=None, video_id=None, all=False):
                """Get product ID from SQL using image ID. If not found, return None."""
                if all:
                    query = f"""SELECT PRODUCT_ID FROM {Table.Middleware.products} """
                    prod_id_res = Database.query(query)
                    if prod_id_res is not None:
                        return [x[0] for x in prod_id_res]

                product_query = None

                if item_no:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Table.Middleware.products} WHERE ITEM_NO = '{item_no}'"
                    )
                if image_id:
                    product_query = f'SELECT PRODUCT_ID FROM {Table.Middleware.images} WHERE IMAGE_ID = {image_id}'
                if video_id:
                    product_query = f'SELECT PRODUCT_ID FROM {Table.Middleware.videos} WHERE VIDEO_ID = {video_id}'
                if binding_id:
                    product_query = (
                        f"SELECT PRODUCT_ID FROM {Table.Middleware.products} WHERE BINDING_ID = '{binding_id}'"
                    )

                if product_query:
                    prod_id_res = Database.query(product_query)
                    if prod_id_res is not None:
                        return prod_id_res[0][0]

                else:
                    Database.logger.warn('No product ID found for the given parameters.')
                    return None

            def get_parent_item_no(product_id=None, binding_id=None, eh=ProcessOutErrorHandler):
                if not product_id and not binding_id:
                    eh.error_handler.add_error_v(
                        'You must provide either product ID or binding ID', origin='get_parent_item_no'
                    )
                    return
                if product_id:
                    query = f"""
                            SELECT ITEM_NO FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id} AND (BINDING_ID IS NULL OR IS_PARENT = 1)
                            """
                elif binding_id:
                    query = f"""
                            SELECT ITEM_NO FROM {Table.Middleware.products}
                            WHERE BINDING_ID = '{binding_id}' AND IS_PARENT = 1
                            """
                response = Database.query(query)

                if response is not None:
                    try:
                        return response[0][0]
                    except:
                        return None

            def get_sku(product_id):
                if product_id:
                    query = f"""
                            SELECT ITEM_NO FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id}
                            """
                else:
                    Database.logger.warn('No product ID provided for SKU lookup.')
                    return
                response = Database.query(query)
                if response is not None:
                    try:
                        return response[0][0]
                    except:
                        return None

            def get_binding_id(product_id):
                if product_id:
                    query = f"""
                            SELECT BINDING_ID FROM {Table.Middleware.products}
                            WHERE PRODUCT_ID = {product_id}
                            """
                else:
                    Database.logger.warn('No product ID provided for binding ID lookup.')
                    return
                response = Database.query(query)
                if response is not None:
                    try:
                        return response[0][0]
                    except KeyError:
                        return None

            def sync(product, eh=ProcessOutErrorHandler, verbose=False):
                for variant in product.variants:
                    if variant.mw_db_id:
                        Database.Shopify.Product.Variant.update(product=product, variant=variant)
                    else:
                        Database.Shopify.Product.Variant.insert(product=product, variant=variant)

                if product.media:
                    for m in product.media:
                        if m.db_id is None:
                            if m.type == 'IMAGE':
                                Database.Shopify.Product.Media.Image.insert(m, eh=eh, verbose=verbose)
                            elif m.type == 'EXTERNAL_VIDEO':
                                Database.Shopify.Product.Media.Video.insert(m, eh=eh, verbose=verbose)
                        else:
                            if m.type == 'IMAGE':
                                Database.Shopify.Product.Media.Image.update(m, eh=eh, verbose=verbose)
                            elif m.type == 'EXTERNAL_VIDEO':
                                Database.Shopify.Product.Media.Video.update(m, eh=eh, verbose=verbose)

            def insert(product):
                for variant in product.variants:
                    Database.Shopify.Product.Variant.insert(product, variant)

            def delete(product_id):
                if product_id:
                    query = f'DELETE FROM {Table.Middleware.products} WHERE PRODUCT_ID = {product_id}'
                else:
                    Database.logger.warn('No product ID provided for deletion.')
                    return

                response = Database.query(query)

                if response['code'] == 200:
                    Database.logger.success(f'Product {product_id} deleted from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'Product {product_id} not found in Middleware.')
                else:
                    error = f'Error deleting product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                    Database.error_handler.add_error_v(error=error)
                    raise Exception(error)

                Database.Shopify.Product.Media.Image.delete(product_id=product_id)
                Database.Shopify.Product.Media.Video.delete(product_id=product_id)

            def get_by_collection_id(collection_id: int):
                query = f"""
                SELECT PRODUCT_ID FROM {Table.Middleware.products}
                WHERE COLLECTION_IDS LIKE '%{collection_id}%'
                """
                response = Database.query(query)
                return [x[0] for x in response] if response else None

            def get_collection_ids(item_no=None, binding_id=None, product_id=None):
                if item_no:
                    query = f"""
                    SELECT CATEG_ID FROM {Table.Middleware.products}
                    WHERE ITEM_NO = '{item_no}'
                    """
                elif binding_id:
                    query = f"""
                    SELECT CATEG_ID FROM {Table.Middleware.products}
                    WHERE BINDING_ID = '{binding_id}'
                    """
                elif product_id:
                    query = f"""
                    SELECT CATEG_ID FROM {Table.Middleware.products}
                    WHERE PRODUCT_ID = {product_id}
                    """
                else:
                    Database.logger.warn('No item number or binding ID provided for lookup.')
                    return

                response = Database.query(query)
                try:
                    return [int(x) for x in response[0][0].split(',')]
                except:
                    return []

            def add_collection_id(collection_id: int, item_no=None, binding_id=None, product_id=None, eh=None):
                if eh is None:
                    eh = Database

                current_collection_ids = Database.Shopify.Product.get_collection_ids(
                    item_no=item_no, binding_id=binding_id, product_id=product_id
                )
                if collection_id not in current_collection_ids:
                    current_collection_ids.append(collection_id)

                    query = f"""
                    UPDATE {Table.Middleware.products}
                    SET CATEG_ID = '{','.join([str(x) for x in current_collection_ids])}'
                    WHERE ITEM_NO = '{item_no}'
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        eh.logger.success(f'Collection ID added to {item_no}')
                    elif response['code'] == 201:
                        return
                    else:
                        eh.error_handler.add_error_v(
                            error=f'Error adding collection ID to {item_no}. Response: {response["message"]}'
                        )

            def remove_collection_id(collection_id: int, item_no=None, binding_id=None, product_id=None, eh=None):
                if eh is None:
                    eh = Database

                current_collection_ids = Database.Shopify.Product.get_collection_ids(
                    item_no=item_no, binding_id=binding_id, product_id=product_id
                )
                if collection_id in current_collection_ids:
                    current_collection_ids.remove(collection_id)
                    query = f"""
                    UPDATE {Table.Middleware.products}
                    SET CATEG_ID = '{','.join([str(x) for x in current_collection_ids])}'
                    WHERE ITEM_NO = '{item_no}'
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        eh.logger.success(f'Collection ID removed from {item_no}')
                    else:
                        eh.error_handler.add_error_v(
                            error=f'Error removing collection ID from {item_no}. Response: {response["message"]}'
                        )

            class Variant:
                def get_id(sku):
                    if sku:
                        query = f"""
                            SELECT VARIANT_ID FROM {Table.Middleware.products}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for variant ID lookup.')
                        return
                    response = Database.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_id(sku):
                    if sku:
                        query = f"""
                            SELECT OPTION_ID FROM {Table.Middleware.products}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for option ID lookup.')
                        return
                    response = Database.query(query)
                    if response is not None:
                        return response[0][0]

                def get_option_value_id(sku):
                    if sku:
                        query = f"""
                            SELECT OPTION_VALUE_ID FROM {Table.Middleware.products}
                            WHERE ITEM_NO = '{sku}'
                            """
                    else:
                        Database.logger.warn('No SKU provided for option value ID lookup.')
                        return
                    response = Database.query(query)
                    if response is not None:
                        return response[0][0]

                def insert(product, variant, eh=ProcessOutErrorHandler, verbose=False):
                    if product.shopify_collections:
                        collection_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        collection_string = None

                    insert_query = f"""
                        INSERT INTO {Table.Middleware.products} (ITEM_NO, BINDING_ID, IS_PARENT, 
                        PRODUCT_ID, VARIANT_ID, INVENTORY_ID, VARIANT_NAME, OPTION_ID, OPTION_VALUE_ID, CATEG_ID, 
                        CF_BOTAN_NAM, CF_PLANT_TYP, CF_HEIGHT, CF_WIDTH, CF_CLIM_ZON, CF_CLIM_ZON_LST,
                        CF_COLOR, CF_SIZE, CF_BLOOM_SEAS, CF_BLOOM_COLOR, CF_LIGHT_REQ, CF_FEATURES, CF_IS_PREORDER, 
                        CF_PREORDER_DT, CF_PREORDER_MSG, CF_IS_FEATURED, CF_IN_STORE_ONLY, CF_IS_ON_SALE, CF_SALE_DESCR,
                        CF_VAR_SIZE, CF_IS_NEW, CF_IS_BACK_IN_STOCK
                        )
                         
                        VALUES ('{variant.sku}', {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        {1 if variant.is_parent else 0}, {product.product_id if product.product_id else "NULL"}, 
                        {variant.variant_id if variant.variant_id else "NULL"}, 
                        {variant.inventory_id if variant.inventory_id else "NULL"}, 
                        {f"'{variant.variant_name}'" if variant.variant_name else "NULL"}, 
                        {variant.option_id if variant.option_id else "NULL"}, 
                        {variant.option_value_id if variant.option_value_id else "NULL"}, 
                        {f"'{collection_string}'" if collection_string else "NULL"},
                        {product.meta_botanical_name['id'] if product.meta_botanical_name['id'] else "NULL"},
                        {product.meta_plant_type['id'] if product.meta_plant_type['id'] else "NULL"},
                        {product.meta_height['id'] if product.meta_height['id'] else "NULL"},
                        {product.meta_width['id'] if product.meta_width['id'] else "NULL"},
                        {product.meta_climate_zone['id'] if product.meta_climate_zone['id'] else "NULL"},
                        {product.meta_climate_zone_list['id'] if product.meta_climate_zone_list['id'] else "NULL"},
                        {product.meta_colors['id'] if product.meta_colors['id'] else "NULL"},
                        {product.meta_size['id'] if product.meta_size['id'] else "NULL"},
                        {product.meta_bloom_season['id'] if product.meta_bloom_season['id'] else "NULL"},
                        {product.meta_bloom_color['id'] if product.meta_bloom_color['id'] else "NULL"},
                        {product.meta_light_requirements['id'] if product.meta_light_requirements['id'] else "NULL"},
                        {product.meta_features['id'] if product.meta_features['id'] else "NULL"},
                        {product.meta_is_preorder['id'] if product.meta_is_preorder['id'] else "NULL"},
                        {product.meta_preorder_release_date['id'] if product.meta_preorder_release_date['id'] else "NULL"},
                        {product.meta_preorder_message['id'] if product.meta_preorder_message['id'] else "NULL"},
                        {product.meta_is_featured['id'] if product.meta_is_featured['id'] else "NULL"},
                        {product.meta_in_store_only['id'] if product.meta_in_store_only['id'] else "NULL"},
                        {product.meta_is_on_sale['id'] if product.meta_is_on_sale['id'] else "NULL"},
                        {product.meta_sale_description['id'] if product.meta_sale_description['id'] else "NULL"},
                        {variant.meta_variant_size['id'] if variant.meta_variant_size['id'] else "NULL"},
                        {product.meta_is_new['id'] if product.meta_is_new['id'] else "NULL"},
                        {product.meta_is_back_in_stock['id'] if product.meta_is_back_in_stock['id'] else "NULL"}
                        )
                        """
                    response = Database.query(insert_query)
                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(
                                f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - INSERT Variant {product.sku}'
                            )
                    else:
                        error = f'Query: {insert_query}\n\nResponse: {response}'
                        eh.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.insert(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def update(product, variant, eh=ProcessOutErrorHandler, verbose=False):
                    if product.shopify_collections:
                        collection_string = ','.join(str(x) for x in product.shopify_collections)
                    else:
                        collection_string = None

                    update_query = f"""
                        UPDATE {Table.Middleware.products} 
                        SET ITEM_NO = '{variant.sku}', 
                        BINDING_ID = {f"'{product.binding_id}'" if product.binding_id else 'NULL'}, 
                        IS_PARENT = {1 if variant.is_parent else 0}, 
                        PRODUCT_ID = {product.product_id if product.product_id else 'NULL'}, 
                        VARIANT_ID = {variant.variant_id if variant.variant_id else 'NULL'}, 
                        INVENTORY_ID = {variant.inventory_id if variant.inventory_id else 'NULL'}, 
                        VARIANT_NAME = {f"'{variant.variant_name}'" if variant.variant_name else "NULL"}, 
                        OPTION_ID = {variant.option_id if variant.option_id else "NULL"}, 
                        OPTION_VALUE_ID = {variant.option_value_id if variant.option_value_id else "NULL"},  
                        CATEG_ID = {f"'{collection_string}'" if collection_string else "NULL"}, 
                        CF_BOTAN_NAM = {product.meta_botanical_name['id'] if product.meta_botanical_name['id'] else "NULL"},
                        CF_PLANT_TYP = {product.meta_plant_type['id'] if product.meta_plant_type['id'] else "NULL"},
                        CF_HEIGHT = {product.meta_height['id'] if product.meta_height['id'] else "NULL"},
                        CF_WIDTH = {product.meta_width['id'] if product.meta_width['id'] else "NULL"},
                        CF_CLIM_ZON = {product.meta_climate_zone['id'] if product.meta_climate_zone['id'] else "NULL"},
                        CF_CLIM_ZON_LST = {product.meta_climate_zone_list['id'] if product.meta_climate_zone_list['id'] else "NULL"},
                        CF_COLOR = {product.meta_colors['id'] if product.meta_colors['id'] else "NULL"},
                        CF_SIZE = {product.meta_size['id'] if product.meta_size['id'] else "NULL"},
                        CF_BLOOM_SEAS = {product.meta_bloom_season['id'] if product.meta_bloom_season['id'] else "NULL"},
                        CF_BLOOM_COLOR = {product.meta_bloom_color['id'] if product.meta_bloom_color['id'] else "NULL"},
                        CF_LIGHT_REQ = {product.meta_light_requirements['id'] if product.meta_light_requirements['id'] else "NULL"},
                        CF_FEATURES = {product.meta_features['id'] if product.meta_features['id'] else "NULL"},
                        CF_IS_PREORDER = {product.meta_is_preorder['id'] if product.meta_is_preorder['id'] else "NULL"},
                        CF_PREORDER_DT = {product.meta_preorder_release_date['id'] if product.meta_preorder_release_date['id'] else "NULL"},
                        CF_PREORDER_MSG = {product.meta_preorder_message['id'] if product.meta_preorder_message['id'] else "NULL"},
                        CF_IS_FEATURED = {product.meta_is_featured['id'] if product.meta_is_featured['id'] else "NULL"},
                        CF_IN_STORE_ONLY = {product.meta_in_store_only['id'] if product.meta_in_store_only['id'] else "NULL"},
                        CF_IS_ON_SALE = {product.meta_is_on_sale['id'] if product.meta_is_on_sale['id'] else "NULL"},
                        CF_SALE_DESCR = {product.meta_sale_description['id'] if product.meta_sale_description['id'] else "NULL"},
                        CF_VAR_SIZE = {variant.meta_variant_size['id'] if variant.meta_variant_size['id'] else "NULL"},
                        CF_IS_NEW = {product.meta_is_new['id'] if product.meta_is_new['id'] else "NULL"},
                        CF_IS_BACK_IN_STOCK = {product.meta_is_back_in_stock['id'] if product.meta_is_back_in_stock['id'] else "NULL"},
                        LST_MAINT_DT = GETDATE() 
                        WHERE ID = {variant.mw_db_id}
                        """
                    response = Database.query(update_query)
                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(
                                f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant'
                            )
                    elif response['code'] == 201:
                        eh.logger.warn(
                            f'SKU: {variant.sku}, Binding ID: {variant.binding_id} - UPDATE Variant: No Rows Affected'
                        )
                    else:
                        error = f'Query: {update_query}\n\nResponse: {response}'
                        eh.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Variant.update(SKU: {variant.sku})'
                        )
                        raise Exception(error)

                def update_metafields(
                    sku,
                    meta_botanical_name=None,
                    meta_plant_type=None,
                    meta_height=None,
                    meta_width=None,
                    meta_climate_zone=None,
                    meta_climate_zone_list=None,
                    meta_colors=None,
                    meta_size=None,
                    meta_bloom_season=None,
                    meta_bloom_color=None,
                    meta_light_requirements=None,
                    meta_features=None,
                    meta_is_preorder=None,
                    meta_preorder_release_date=None,
                    meta_preorder_message=None,
                    meta_is_featured=None,
                    meta_in_store_only=None,
                    meta_is_on_sale=None,
                    meta_sale_description=None,
                    meta_variant_size=None,
                    meta_is_new=None,
                    meta_is_back_in_stock=None,
                    update_timestamp=True,
                    verbose=False,
                    eh=ProcessInErrorHandler,
                ):
                    # Build the query
                    timestamp = ''
                    if update_timestamp:
                        timestamp = 'LST_MAINT_DT = GETDATE() '

                    fields = ''
                    if meta_botanical_name:
                        fields += f"CF_BOTAN_NAM = '{meta_botanical_name}', "
                    if meta_plant_type:
                        fields += f"CF_PLANT_TYP = '{meta_plant_type}', "
                    if meta_height:
                        fields += f"CF_HEIGHT = '{meta_height}', "
                    if meta_width:
                        fields += f"CF_WIDTH = '{meta_width}', "
                    if meta_climate_zone:
                        fields += f"CF_CLIM_ZON = '{meta_climate_zone}', "
                    if meta_climate_zone_list:
                        fields += f"CF_CLIM_ZON_LST = '{meta_climate_zone_list}', "
                    if meta_colors:
                        fields += f"CF_COLOR = '{meta_colors}', "
                    if meta_size:
                        fields += f"CF_SIZE = '{meta_size}', "
                    if meta_bloom_season:
                        fields += f"CF_BLOOM_SEAS = '{meta_bloom_season}', "
                    if meta_bloom_color:
                        fields += f"CF_BLOOM_COLOR = '{meta_bloom_color}', "
                    if meta_light_requirements:
                        fields += f"CF_LIGHT_REQ = '{meta_light_requirements}', "
                    if meta_features:
                        fields += f"CF_FEATURES = '{meta_features}', "
                    if meta_is_preorder:
                        fields += f"CF_IS_PREORDER = '{meta_is_preorder}', "
                    if meta_preorder_release_date:
                        fields += f"CF_PREORDER_DT = '{meta_preorder_release_date}', "
                    if meta_preorder_message:
                        fields += f"CF_PREORDER_MSG = '{meta_preorder_message}', "
                    if meta_is_featured:
                        fields += f"CF_IS_FEATURED = '{meta_is_featured}', "
                    if meta_in_store_only:
                        fields += f"CF_IN_STORE_ONLY = '{meta_in_store_only}', "
                    if meta_is_on_sale:
                        fields += f"CF_IS_ON_SALE = '{meta_is_on_sale}', "
                    if meta_sale_description:
                        fields += f"CF_SALE_DESCR = '{meta_sale_description}', "
                    if meta_variant_size:
                        fields += f"CF_VAR_SIZE = '{meta_variant_size}', "
                    if meta_is_new:
                        fields += f"CF_IS_NEW = '{meta_is_new}', "
                    if meta_is_back_in_stock:
                        fields += f"CF_IS_BACK_IN_STOCK = '{meta_is_back_in_stock}', "

                    if fields[-2:] == ', ':
                        fields = fields[:-2]

                    update_query = f"""
                        UPDATE {Table.Middleware.products} 
                        SET  
                        {fields}
                        {timestamp} 
                        WHERE ITEM_NO = '{sku}'
                        """
                    response = Database.query(update_query)
                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(f'SKU: {sku} - Metafields updated in middleware')

                    elif response['code'] == 201:
                        eh.logger.warn(f'SKU: {sku},  - UPDATE Metafields: No Rows Affected')

                    else:
                        error = f'Query: {update_query}\n\nResponse: {response}'

                        eh.error_handler.add_error_v(
                            error=error, origin=f'Database.Shopify.Product.Variant.update(SKU: {sku})'
                        )

                        raise Exception(error)

                def delete(variant_id, eh=ProcessOutErrorHandler, verbose=False):
                    if variant_id:
                        query = f'DELETE FROM {Table.Middleware.products} WHERE VARIANT_ID = {variant_id}'
                    else:
                        eh.logger.warn('No variant ID provided for deletion.')
                        return
                    response = Database.query(query)

                    if response['code'] == 200:
                        if verbose:
                            eh.logger.success(f'Variant {variant_id} deleted from Middleware.')
                    else:
                        error = f'Error deleting variant {variant_id} from Middleware. \n Query: {query}\nResponse: {response}'
                        eh.error_handler.add_error_v(error=error)
                        raise Exception(error)

                class Media:
                    class Image:
                        def get(item_no):
                            """Return all image ids for a product."""
                            if item_no:
                                query = f"""
                                SELECT IMAGE_ID FROM {Table.Middleware.images}
                                WHERE ITEM_NO = '{item_no}'
                                """
                            else:
                                Database.logger.warn('No SKU provided for image ID lookup.')
                                return
                            response = Database.query(query)
                            if response:
                                return [x[0] for x in response] if response else None

            class Media:
                def delete(product_id):
                    Database.Shopify.Product.Media.Image.delete(product_id=product_id)
                    Database.Shopify.Product.Media.Video.delete(product_id=product_id)

                class Image:
                    def get(image_id=None, column=None):
                        if column is None:
                            column = '*'  # Get all columns

                        if image_id:
                            where_filter = f'WHERE IMAGE_ID = {image_id}'
                        else:
                            where_filter = ''  # Get all images

                        query = f"""
                        SELECT {column} 
                        FROM {Table.Middleware.images}
                        {where_filter}
                        """
                        response = Database.query(query)
                        if column == '*' or ',' in column or where_filter == '':  # Return all columns
                            return response
                        else:
                            try:
                                return response[0][0]
                            except:
                                return None

                    def get_image_id(file_name):
                        if file_name:
                            query = (
                                f"SELECT IMAGE_ID FROM {Table.Middleware.images} WHERE IMAGE_NAME = '{file_name}'"
                            )
                        else:
                            Database.logger.warn('No file name provided for image ID lookup.')
                            return
                        try:
                            img_id_res = Database.query(query)
                            return img_id_res[0][0]
                        except:
                            return None

                    def insert(image, eh=ProcessOutErrorHandler, verbose=False):
                        img_insert = f"""
                        INSERT INTO {Table.Middleware.images} (IMAGE_NAME, ITEM_NO, FILE_PATH,
                        PRODUCT_ID, IMAGE_ID, THUMBNAIL, IMAGE_NUMBER, SORT_ORDER,
                        IS_BINDING_IMAGE, BINDING_ID, IS_VARIANT_IMAGE, DESCR, SIZE)
                        VALUES (
                        '{image.name}', {f"'{image.sku}'" if image.sku else 'NULL'},
                        '{image.file_path}', {image.product_id}, {image.shopify_id}, '{1 if image.is_thumbnail else 0}', 
                        '{image.number}', '{image.sort_order}', '{image.is_binding_image}',
                        {f"'{image.binding_id}'" if image.binding_id else 'NULL'}, '{image.is_variant_image}',
                        {f"'{Database.sql_scrub(image.description)}'" if image.description != '' else 'NULL'},
                        {image.size})"""
                        insert_img_response = Database.query(img_insert)
                        if insert_img_response['code'] == 200:
                            if verbose:
                                eh.logger.success(f'SQL INSERT Image {image.name}: Success')
                        else:
                            error = f'Error inserting image {image.name} into Middleware. \nQuery: {img_insert}\nResponse: {insert_img_response}'
                            eh.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.insert({image.name})'
                            )
                            raise Exception(error)

                    def update(image, eh=ProcessOutErrorHandler, verbose=False):
                        q = f"""
                        UPDATE {Table.Middleware.images}
                        SET IMAGE_NAME = '{image.name}', ITEM_NO = '{image.sku}', FILE_PATH = '{image.file_path}',
                        PRODUCT_ID = '{image.product_id}', IMAGE_ID = '{image.shopify_id}',
                        THUMBNAIL = '{1 if image.is_thumbnail else 0}', IMAGE_NUMBER = '{image.number}',
                        SORT_ORDER = '{image.sort_order}', IS_BINDING_IMAGE = '{image.is_binding_image}',
                        BINDING_ID = {f"'{image.binding_id}'" if image.binding_id else 'NULL'},
                        IS_VARIANT_IMAGE = '{image.is_variant_image}',
                        DESCR = {f"'{Database.sql_scrub(image.description)}'" if
                                    image.description != '' else 'NULL'}, SIZE = '{image.size}'
                        WHERE ID = {image.db_id}"""

                        res = Database.query(q)
                        if res['code'] == 200:
                            if verbose:
                                eh.logger.success(f'SQL UPDATE Image {image.name}: Success')
                        elif res['code'] == 201:
                            eh.logger.warn(f'SQL UPDATE Image {image.name}: Not found')
                        else:
                            error = (
                                f'Error updating image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                            )
                            eh.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.update({image.name})'
                            )
                            raise Exception(error)

                    def delete(
                        image=None,
                        image_id=None,
                        image_name=None,
                        product_id=None,
                        eh=ProcessOutErrorHandler,
                        verbose=False,
                    ):
                        prod_id = None

                        if image_id:
                            # Delete Single Image from Image ID
                            prod_id = Database.Shopify.Product.Media.Image.get(
                                image_id=image_id, column='PRODUCT_ID'
                            )
                            sort_order = Database.Shopify.Product.Media.Image.get(
                                image_id=image_id, column='SORT_ORDER'
                            )
                            q = f"DELETE FROM {Table.Middleware.images} WHERE IMAGE_ID = '{image_id}'"

                        elif image:
                            # Delete Single Image from Image Object
                            prod_id = image.product_id
                            sort_order = image.sort_order
                            if image.shopify_id is None:
                                image.shopify_id = Database.Shopify.Product.Media.Image.get_image_id(
                                    filename=image.name
                                )
                            q = f"DELETE FROM {Table.Middleware.images} WHERE IMAGE_ID = '{image.shopify_id}'"

                        elif product_id:
                            # Delete All Images from Product ID
                            q = f"DELETE FROM {Table.Middleware.images} WHERE PRODUCT_ID = '{product_id}'"

                        else:
                            if verbose:
                                eh.logger.warn('No image or image_id provided for deletion.')
                            return

                        res = Database.query(q)
                        if res['code'] == 200:
                            if verbose:
                                eh.logger.success(f"""{res['affected rows']} images deleted from Middleware.""")
                            if (image_id or image) and prod_id:
                                # Decrement sort order of remaining images
                                query = f"""
                                UPDATE {Table.Middleware.images}
                                SET SORT_ORDER = SORT_ORDER - 1
                                WHERE PRODUCT_ID = {prod_id} AND SORT_ORDER > {sort_order}
                                """
                                response = Database.query(query)
                                if response['code'] == 200:
                                    if verbose:
                                        eh.logger.success('Decrement Photos: Success')
                                elif response['code'] == 201:
                                    if verbose:
                                        eh.logger.warn('Decrement Photos: No Rows Affected')
                                else:
                                    error = f'Error decrementing sort order of images in Middleware. \nQuery: {query}\nResponse: {response}'
                                    eh.error_handler.add_error_v(
                                        error=error,
                                        origin=f'Database.Shopify.Product.Media.Image.delete(query:\n{q})',
                                    )
                                    raise Exception(error)
                        elif res['code'] == 201:
                            if verbose:
                                eh.logger.warn(f'IMAGE DELETE: Not found\n\nQuery: {q}\n')
                        else:
                            if image:
                                error = f'Error deleting image {image.name} in Middleware. \nQuery: {q}\nResponse: {res}'
                            elif image_id:
                                error = f'Error deleting image with ID {image_id} in Middleware. \nQuery: {q}\nResponse: {res}'
                            elif product_id:
                                error = f'Error deleting images for product {product_id} in Middleware. \nQuery: {q}\nResponse: {res}'
                            eh.error_handler.add_error_v(
                                error=error, origin=f'Database.Shopify.Product.Media.Image.delete(query:\n{q})'
                            )
                            raise Exception(error)

                class Video:
                    def get(product_id=None, video_id=None, url=None, sku=None, column=None):
                        if column is None:
                            column = '*'

                        if product_id:
                            where_filter = f'WHERE PRODUCT_ID = {product_id}'
                        elif video_id:
                            where_filter = f'WHERE VIDEO_ID = {video_id}'
                        elif url and sku:
                            where_filter = f"WHERE URL = '{url}' AND ITEM_NO = '{sku}'"
                        else:
                            where_filter = ''  # Get all videos

                        query = f"""
                        SELECT {column} 
                        FROM {Table.Middleware.videos}
                        {where_filter}
                        """

                        response = Database.query(query)
                        if response:
                            if product_id:
                                return response
                            elif where_filter == '':  # Return all columns
                                return response
                            else:
                                try:
                                    return response[0][0]
                                except:
                                    return None

                    def insert(video, eh=ProcessOutErrorHandler, verbose=False):
                        query = f"""
                        INSERT INTO {Table.Middleware.videos} (ITEM_NO, URL, VIDEO_NAME, FILE_PATH, 
                        PRODUCT_ID, VIDEO_ID, SORT_ORDER, BINDING_ID, DESCR, SIZE)
                        VALUES (
                        {f"'{video.sku}'" if video.sku else 'NULL'},
                        {f"'{video.url}'" if video.url else 'NULL'},
                        {f"'{video.name}'" if video.name else 'NULL'}, 
                        {f"'{video.file_path}'" if video.file_path else 'NULL'}, 
                        {video.product_id}, 
                        {video.shopify_id}, 
                        {video.sort_order}, {f"'{video.binding_id}'" if video.binding_id else 'NULL'}, 
                        {f"'{video.description}'" if video.description else 'NULL'}, {video.size if video.size else 'NULL'})
                        """
                        response = Database.query(query)
                        if response['code'] == 200:
                            if verbose:
                                eh.logger.success(f'Video {video.shopify_id} added to Middleware.')
                        else:
                            error = f'Error adding video {video.shopify_id} to Middleware. \nQuery: {query}\nResponse: {response}'
                            eh.error_handler.add_error_v(error=error)
                            raise Exception(error)

                    def update(video, eh=ProcessOutErrorHandler, verbose=False):
                        query = f"""
                        UPDATE {Table.Middleware.videos}
                        SET 
                        ITEM_NO = {f"'{video.sku}'" if video.sku else 'NULL'},
                        URL = {f"'{video.url}'" if video.url else 'NULL'},
                        VIDEO_NAME = {f"'{video.name}'" if video.name else 'NULL'}, 
                        FILE_PATH = {f"'{video.file_path}'" if video.file_path else 'NULL'}, 
                        PRODUCT_ID = {video.product_id}, 
                        VIDEO_ID = {video.shopify_id}, 
                        SORT_ORDER = {video.sort_order}, 
                        BINDING_ID = {f"'{video.binding_id}'" if video.binding_id else 'NULL'}, 
                        DESCR = {f"'{video.description}'" if video.description else 'NULL'}, 
                        SIZE = {video.size if video.size else 'NULL'}
                        WHERE ID = {video.db_id}
                        """
                        response = Database.query(query)
                        if response['code'] == 200:
                            if verbose:
                                eh.logger.success(f'Video {video.shopify_id} updated in Middleware.')
                        elif response['code'] == 201:
                            eh.logger.warn(f'UPDATE: Video {video.shopify_id} not found.\n\nQuery: {query}')
                        else:
                            error = f'Error updating video {video.shopify_id} in Middleware. \nQuery: {query}\nResponse: {response}'
                            eh.error_handler.add_error_v(error=error)
                            raise Exception(error)

                    def delete(
                        video=None,
                        video_id=None,
                        url=None,
                        sku=None,
                        product_id=None,
                        eh=ProcessOutErrorHandler,
                        verbose=False,
                    ):
                        """Delete video from Middleware and decrement the sort order of remaining item videos."""
                        sort_order = None
                        if video:  # Video Object
                            sort_order = video.sort_order
                            product_id = video.product_id
                            where_filter = f'WHERE VIDEO_ID = {video.shopify_id}'
                        else:
                            if video_id:
                                product_id = Database.Shopify.Product.get_id(video_id=video_id)
                                sort_order = Database.Shopify.Product.Media.Video.get(
                                    video_id=video_id, column='SORT_ORDER'
                                )
                                where_filter = f'WHERE VIDEO_ID = {video_id}'
                            elif url and sku:
                                product_id = Database.Shopify.Product.get_id(item_no=sku)
                                sort_order = Database.Shopify.Product.Media.Video.get(
                                    url=url, sku=sku, column='SORT_ORDER'
                                )
                                where_filter = f"WHERE URL = '{url}' AND ITEM_NO = '{sku}'"
                            elif product_id:
                                where_filter = f'WHERE PRODUCT_ID = {product_id}'
                            else:
                                raise Exception('No video_id or product_id provided for deletion.')

                        query = f'DELETE FROM {Table.Middleware.videos} {where_filter}'
                        response = Database.query(query)
                        if response['code'] == 200:
                            if video_id:
                                if verbose:
                                    eh.logger.success(f'Video {video_id} deleted from Middleware.')
                            elif url and sku:
                                eh.logger.success(f'Video {url} for product {sku} deleted from Middleware.')

                            if verbose:
                                eh.logger.success(
                                    f"""{response['affected rows']} videos deleted from Middleware."""
                                )

                            if product_id and sort_order:
                                # Decrement sort order of remaining videos
                                query = f"""
                                UPDATE {Table.Middleware.videos}
                                SET SORT_ORDER = SORT_ORDER - 1
                                WHERE PRODUCT_ID = {product_id} AND SORT_ORDER > {sort_order}
                                """
                                decrement_response = Database.query(query)
                                if decrement_response['code'] == 200:
                                    if verbose:
                                        eh.logger.success('Sort order decremented for remaining videos.')
                                elif decrement_response['code'] == 201:
                                    if verbose:
                                        eh.logger.warn('No rows affected for sort order decrement.')
                                else:
                                    error = f'Error decrementing sort order for remaining videos. \nQuery: {query}\nResponse: {decrement_response}'
                                    eh.error_handler.add_error_v(error=error)
                                    raise Exception(error)

                        elif response['code'] == 201:
                            if verbose:
                                if video_id:
                                    eh.logger.warn(f'DELETE: Video {video_id} not found.')
                                elif product_id:
                                    eh.logger.warn(
                                        f'Videos for product {product_id} not found in Middleware.\n\nQuery: {query}\n'
                                    )
                                elif url and sku:
                                    eh.logger.warn(
                                        f'Video {url} for product {sku} not found in Middleware.\n\nQuery: {query}\n'
                                    )
                        else:
                            if video_id:
                                error = f'Error deleting video {video_id} from Middleware. \n Query: {query}\nResponse: {response}'
                            elif product_id:
                                error = f'Error deleting videos for product {product_id} from Middleware. \n Query: {query}\nResponse: {response}'
                            elif url and sku:
                                error = f'Error deleting video {url} for product {sku} from Middleware. \n Query: {query}\nResponse: {response}'
                            eh.error_handler.add_error_v(error=error)
                            raise Exception(error)

            class Metafield:
                def delete(sku: str = None, shopify_product_id: int = None, column=None):
                    if not sku or not shopify_product_id:
                        Database.logger.warn('No SKU or Shopify Product ID provided for metafield deletion.')
                        return
                    query = f'UPDATE {Table.Middleware.products} SET'

                    if column:
                        query += f' {column} = NULL'
                    else:
                        # Delete All Metafield Column Data for Customer
                        prod_meta_columns = creds.Shopify.Metafield.Product.__dict__.keys()
                        if prod_meta_columns:
                            for column in prod_meta_columns:
                                if not column.startswith('__'):
                                    key = creds.Shopify.Metafield.Product.__dict__[column]
                                    query += f' {key} = NULL,'
                            # Remove trailing comma
                            if query[-1] == ',':
                                query = query[:-1]

                    if shopify_product_id:
                        query += f' WHERE PRODUCT_ID = {shopify_product_id}'
                    elif sku:
                        query += f" WHERE ITEM_NO = '{sku}'"

                    response = Database.query(query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'Metafield {column} deleted from product {shopify_product_id or sku}.'
                        )
                    elif response['code'] == 201:
                        Database.logger.warn(
                            f'No rows affected for product {shopify_product_id or sku} in Middleware.'
                        )
                    else:
                        raise Exception(response['message'])

        class Metafield_Definition:
            def get(definition_id=None):
                if definition_id:
                    where_filter = f'WHERE META_ID = {definition_id}'
                else:
                    where_filter = ''

                query = f'SELECT * FROM {Table.Middleware.metafields} {where_filter}'
                response = Database.query(query)
                if response is not None:
                    result = {}
                    for row in response:
                        result[row[1]] = {
                            'META_ID': row[0],
                            'NAME': row[1],
                            'DESCR': row[2],
                            'NAME_SPACE': row[3],
                            'META_KEY': row[4],
                            'TYPE': row[5],
                            'PINNED_POS': row[6],
                            'OWNER_TYPE': row[7],
                            'VALIDATIONS': [
                                {'NAME': row[8], 'TYPE': row[9], 'VALUE': row[10]},
                                {'NAME': row[11], 'TYPE': row[12], 'VALUE': row[13]},
                                {'NAME': row[14], 'TYPE': row[15], 'VALUE': row[16]},
                                {'NAME': row[17], 'TYPE': row[18], 'VALUE': row[19]},
                                {'NAME': row[20], 'TYPE': row[21], 'VALUE': row[22]},
                            ],
                            'PIN': row[23],
                        }
                    return result

            def insert(values):
                number_of_validations = len(values['VALIDATIONS'])
                if values['DESCR']:
                    values['DESCR'] = Database.sql_scrub(values['DESCR'])

                if number_of_validations > 0:
                    validation_columns = ', ' + ', '.join(
                        [
                            f'VALID_{i+1}_NAME, VALID_{i+1}_VALUE, VALID_{i+1}_TYPE'
                            for i in range(number_of_validations)
                        ]
                    )

                    validation_values = ', ' + ', '.join(
                        [
                            f"'{values['VALIDATIONS'][i]['NAME']}', '{values['VALIDATIONS'][i]['VALUE']}', '{values['VALIDATIONS'][i]['TYPE']}'"
                            for i in range(number_of_validations)
                        ]
                    )
                else:
                    validation_columns = ''
                    validation_values = ''

                query = f"""
                        INSERT INTO {Table.Middleware.metafields} (META_ID, NAME, DESCR, NAME_SPACE, META_KEY, 
                        TYPE, PIN, PINNED_POS, OWNER_TYPE {validation_columns})
                        VALUES({values['META_ID']}, '{values['NAME']}', '{values['DESCR']}', 
                        '{values['NAME_SPACE']}', '{values['META_KEY']}', '{values['TYPE']}',
                        {values['PIN']}, {values['PINNED_POS']}, '{values['OWNER_TYPE']}' {validation_values})
                        """

                response = Database.query(query)
                if response['code'] != 200:
                    error = f'Error inserting metafield definition {values["META_ID"]}. \nQuery: {query}\nResponse: {response}'
                    raise Exception(error)

            def update(values):
                query = f"""
                        UPDATE {Table.Middleware.metafields}
                        SET META_ID = {values['META_ID']},
                        NAME =  '{values['NAME']}',
                        DESCR = '{values['DESCR']}',
                        NAME_SPACE = '{values['NAME_SPACE']}',
                        META_KEY = '{values['META_KEY']}',
                        TYPE = '{values['TYPE']}',
                        PINNED_POS = {values['PINNED_POS']},
                        OWNER_TYPE = '{values['OWNER_TYPE']}',
                        {', '.join([f"VALID_{i+1}_NAME = '{values['VALIDATIONS'][i]['NAME']}', VALID_{i+1}_VALUE = '{values['VALIDATIONS'][i]['VALUE']}', VALID_{i+1}_TYPE = '{values['VALIDATIONS'][i]['TYPE']}'" for i in range(len(values['VALIDATIONS']))])},
                        LST_MAINT_DT = GETDATE()
                        WHERE META_ID = {values['META_ID']}

                        """
                response = Database.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete(definition_id=None):
                if definition_id:
                    where_filter = f'WHERE META_ID = {definition_id}'
                else:
                    where_filter = ''
                query = f'DELETE FROM {Table.Middleware.metafields} {where_filter}'
                response = Database.query(query)
                if response['code'] not in [200, 201]:
                    raise Exception(response['message'])

        class Webhook:
            def get(id='', ids_only=False):
                if id:
                    query = f'SELECT * FROM {Table.Middleware.webhooks} WHERE HOOK_ID = {id}'
                    response = Database.query(query)
                    if response is not None:
                        return response
                else:
                    query = f'SELECT * FROM {Table.Middleware.webhooks}'
                    response = Database.query(query)
                    if response is not None:
                        if ids_only:
                            return [hook['id'] for hook in response]
                        return response

            def insert(webhook_data):
                query = f"""
                        INSERT INTO {Table.Middleware.webhooks} (HOOK_ID, TOPIC, DESTINATION, FORMAT, DOMAIN)
                        VALUES ({webhook_data['HOOK_ID']}, '{webhook_data['TOPIC']}', '{webhook_data['DESTINATION']}', '{webhook_data['FORMAT']}', '{webhook_data['DOMAIN']}')
                        """
                response = Database.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def update(webhook_data):
                query = f"""
                        UPDATE {Table.Middleware.webhooks}
                        SET TOPIC = '{webhook_data['TOPIC']}', 
                        DESTINATION = '{webhook_data['DESTINATION']}', 
                        FORMAT = '{webhook_data['FORMAT']}',
                        DOMAIN = '{webhook_data['DOMAIN']}'
                        WHERE HOOK_ID = {webhook_data['HOOK_ID']}
                        """
                response = Database.query(query)
                if response['code'] != 200:
                    raise Exception(response['message'])

            def delete(hook_id=None, all=False):
                if all:
                    response = Database.query(f'DELETE FROM {Table.Middleware.webhooks}')
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return 'All webhooks deleted'
                elif hook_id:
                    response = Database.query(f'DELETE FROM {Table.Middleware.webhooks} WHERE HOOK_ID = {hook_id}')
                    if response['code'] != 200:
                        raise Exception(response['message'])
                    else:
                        return f'Webhook {hook_id} deleted'

        class Promotion:
            """Promotion Prices translated into Automatic Discounts in Shopify."""

            def get(group_code=None):
                if group_code:
                    query = f"SELECT SHOP_ID FROM {Table.Middleware.promotions} WHERE GRP_COD = '{group_code}'"
                else:
                    query = f'SELECT GRP_COD FROM {Table.Middleware.promotions}'
                response = Database.query(query)
                return [x[0] for x in response] if response else None

            def sync(rule):
                if not rule.shopify_id:
                    Database.logger.warn(
                        message='No Shopify ID provided for promotion sync.',
                        origin='Database.Shopify.Promotion.sync',
                    )
                    return
                if rule.db_id:
                    Database.Shopify.Promotion.update(rule)
                else:
                    Database.Shopify.Promotion.insert(rule)

                Database.Shopify.Promotion.BxgyLine.sync(rule)

            def insert(rule):
                """Insert a new discount rule into the Middleware."""
                query = f"""
                INSERT INTO {Table.Middleware.promotions}(GRP_COD, RUL_SEQ_NO, SHOP_ID, ENABLED)
                VALUES('{rule.grp_cod}', '{rule.seq_no}', {rule.seq_no},{rule.shopify_id}, {1 if rule.is_enabled_cp else 0})
                """
                response = Database.query(query)
                if response['code'] == 200:
                    pass
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Insertion',
                    )

            def update(rule):
                query = f"""
                UPDATE {Table.Middleware.promotions}
                SET SHOP_ID = {rule.shopify_id}, 
                ENABLED = {1 if rule.is_enabled_cp else 0}, 
                LST_MAINT_DT = GETDATE()
                WHERE GRP_COD = '{rule.grp_cod}' and RUL_SEQ_NO = '{rule.seq_no}'
                """
                response = Database.query(query)
                if response['code'] == 200:
                    pass
                elif response['code'] == 201:
                    Database.logger.warn(
                        f'Promotion {rule.grp_cod}-Rule: {rule.seq_no} not found for update in Middleware.'
                    )
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Update',
                    )

            def delete(shopify_id):
                if not shopify_id:
                    Database.logger.warn('No Shopify ID provided for deletion.')
                    return
                query = f"""
                DELETE FROM {Table.Middleware.promotions} 
                WHERE SHOP_ID = {shopify_id}

                DELETE FROM {Table.Middleware.promotion_lines_fixed}
                WHERE GRP_COD = (SELECT GRP_COD FROM {Table.Middleware.promotions} 
                                WHERE SHOP_ID = {shopify_id})
                                
                DELETE FROM {Table.Middleware.promotion_lines_bogo}
                WHERE SHOP_ID = {shopify_id}
                """
                response = Database.query(query)

                if response['code'] == 200:
                    Database.logger.success(f'DELETE: Promotion {shopify_id} deleted successfully from Middleware.')
                elif response['code'] == 201:
                    Database.logger.warn(f'DELETE: Promotion {shopify_id} not found in Middleware.')
                else:
                    Database.error_handler.add_error_v(
                        error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                        origin='Middleware Promotion Deletion',
                    )

            def has_fixed_priced_items(rule):
                query = f"""
                SELECT COUNT(*) FROM {Table.Middleware.promotion_lines_fixed} 
                WHERE GRP_COD = '{rule.grp_cod}' AND RUL_SEQ_NO = {rule.seq_no}
                """
                response = Database.query(query)
                if response is not None:
                    if response[0][0] > 0:
                        return True
                return False

            class BxgyLine:
                """Buy X Get Y Line Items"""

                def get(shopify_id):
                    if not shopify_id:
                        return
                    query = (
                        f'SELECT ITEM_NO FROM {Table.Middleware.promotion_lines_bogo} WHERE SHOP_ID = {shopify_id}'
                    )
                    response = Database.query(query)
                    return [x[0] for x in response] if response else None

                def sync(rule):
                    cp_items = rule.items
                    mw_bogo_items = rule.mw_bogo_items
                    if cp_items:
                        for item in cp_items:
                            if mw_bogo_items:
                                if item in mw_bogo_items:
                                    continue
                                else:
                                    Database.Shopify.Promotion.BxgyLine.insert(item, rule.shopify_id)
                            else:
                                Database.Shopify.Promotion.BxgyLine.insert(item, rule.shopify_id)
                    if mw_bogo_items:
                        for item in mw_bogo_items:
                            if item in cp_items:
                                continue
                            else:
                                Database.Shopify.Promotion.BxgyLine.delete(item_no_list=[item])

                def insert(item, shopify_promo_id, rule_seq_no):
                    """Insert Items affected by BOGO promos into middleware."""
                    if not item:
                        Database.logger.warn('No item provided for insertion.')
                        return
                    query = f"""
                    INSERT INTO {Table.Middleware.promotion_lines_bogo} (SHOP_ID, ITEM_NO, RUL_SEQ_NO)
                    VALUES ({shopify_promo_id}, '{item}', '{rule_seq_no}')
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'Promotion {shopify_promo_id} Item: {item} inserted successfully into Middleware.'
                        )
                    else:
                        Database.error_handler.add_error_v(
                            error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                            origin='Middleware Promotion Line Item Insertion',
                        )

                def update(line, shopify_id, rule_seq_no):
                    if not line.item_no:
                        Database.logger.warn('No item number provided for update.')
                        return
                    query = f"""
                    UPDATE {Table.Middleware.promotion_lines_bogo}
                    SET SHOP_ID = {shopify_id}, RUL_SEQ_NO = '{rule_seq_no}'
                    WHERE ITEM_NO = '{line.item_no}'
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        Database.logger.success(f'Promotion {line.item_no} updated successfully in Middleware.')
                    elif response['code'] == 201:
                        Database.logger.warn(f'Promotion {line.item_no} not found for update in Middleware.')
                    else:
                        Database.error_handler.add_error_v(
                            error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                            origin='Middleware Promotion Line Item Update',
                        )

                def delete(shopify_id=None, item_no_list=None):
                    if not shopify_id and not item_no_list:
                        Database.logger.warn('MW Discount Line Delete: Must provide promo id or item number list.')
                        return
                    if shopify_id:
                        # Delete all line items for a specific promotion
                        query = f'DELETE FROM {Table.Middleware.promotion_lines_bogo} WHERE SHOP_ID = {shopify_id}'
                        response = Database.query(query)
                        if response['code'] == 200:
                            Database.logger.success(
                                f'DELETE: Promotion {shopify_id} deleted successfully from Middleware.'
                            )
                        elif response['code'] == 201:
                            Database.logger.warn(f'DELETE: Promotion {shopify_id} not found in Middleware.')
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                                origin='Middleware Promotion Line Item Deletion',
                            )
                        return
                    elif item_no_list:
                        # Delete all line items for a list of item numbers
                        if len(item_no_list) == 1:
                            where_filter = f"ITEM_NO = '{item_no_list[0]}'"
                        else:
                            where_filter = f'ITEM_NO IN {tuple(item_no_list)}'

                        query = f'DELETE FROM {Table.Middleware.promotion_lines_bogo} WHERE {where_filter}'

                        response = Database.query(query)
                        if response['code'] == 200:
                            Database.logger.success(
                                f'DELETE: Promotion {item_no_list} deleted successfully from Middleware.'
                            )
                        elif response['code'] == 201:
                            Database.logger.warn(
                                f'DELETE: No Promotion lines for {item_no_list} found in Middleware.'
                            )
                        else:
                            Database.error_handler.add_error_v(
                                error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                                origin='Middleware Promotion Line Item Deletion',
                            )

            class FixLine:
                """Fixed Price Discount Line Items"""

                def get(group_cod, rule_seq_no) -> list[str]:
                    query = f"""
                    SELECT ITEM_NO FROM {Table.Middleware.promotion_lines_fixed}
                    WHERE GRP_COD = '{group_cod}' AND RUL_SEQ_NO = '{rule_seq_no}'
                    """
                    response = Database.query(query)
                    return [x[0] for x in response] if response else None

                def insert(group_cod, rule_seq_no, item_no):
                    query = f"""
                    INSERT INTO {Table.Middleware.promotion_lines_fixed} (GRP_COD, RUL_SEQ_NO, ITEM_NO)
                    VALUES ('{group_cod}', {rule_seq_no}, '{item_no}')
                    """
                    response = Database.query(query)
                    if response['code'] == 200:
                        Database.logger.success(
                            f'Promotion {group_cod}-Rule: {rule_seq_no} Item: {item_no} inserted successfully into Middleware.'
                        )
                    else:
                        Database.error_handler.add_error_v(
                            error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                            origin='Middleware Promotion Line Item Insertion',
                        )

                def delete(group_cod, rule_seq_no, item_no):
                    query = f"""
                    DELETE FROM {Table.Middleware.promotion_lines_fixed}
                    WHERE GRP_COD = '{group_cod}' AND RUL_SEQ_NO = '{rule_seq_no}' AND ITEM_NO = '{item_no}'
                    """
                    response = Database.query(query)

                    if response['code'] == 200:
                        Database.logger.success(
                            f'Promotion {group_cod}-Rule: {rule_seq_no} Item: {item_no} deleted successfully from Middleware.'
                        )
                    elif response['code'] == 201:
                        Database.logger.warn(
                            f'Promotion {group_cod}-Rule: {rule_seq_no} Item: {item_no} not found for deletion in Middleware.'
                        )
                    else:
                        Database.error_handler.add_error_v(
                            error=f'Error: {response["code"]}\n\nQuery: {query}\n\nResponse:{response["message"]}',
                            origin='Middleware Promotion Line Item Deletion',
                        )

        class Discount:
            """Basic Discount Codes"""

            def get(discount_id):
                if not discount_id:
                    Database.logger.warn('No discount ID provided for lookup.')
                    return
                query = f'SELECT * FROM {Table.Middleware.discounts} WHERE SHOP_ID = {discount_id}'
                response = Database.query(query)
                if response:
                    db_id = response[0][0]
                    shop_id = response[0][1]
                    cp_id = response[0][2]
                    lst_maint_dt = response[0][3]
                    return {'db_id': db_id, 'shop_id': shop_id, 'cp_id': cp_id, 'lst_maint_dt': lst_maint_dt}

        class Gift_Certificate:
            """Requires Shopify Plus Gift Card API Access"""

            pass


if __name__ == '__main__':
    print(Database.CP.Customer.get_loyalty_balance('105786afd'))
