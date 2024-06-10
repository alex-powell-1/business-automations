import requests

import integration.utilities as utilities
from integration.database import Database
from setup import creds
import integration.object_processor as object_processor

from integration.error_handler import ErrorHandler, Logger, GlobalErrorHandler

import time

class GiftCertificates:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database.db

        self.logger = GlobalErrorHandler.logger
        self.error_handler = GlobalErrorHandler.error_handler

        self.certificates = self.get_certificates()
        self.processor = object_processor.ObjectProcessor(objects=self.certificates)

        # self.big_certificates = self.get_certificates_from_big()
        # self.big_processor = object_processor.ObjectProcessor(objects=self.big_certificates)

    def get_certificates(self):
        # query = f"""
        # SELECT GFC_NO, ORIG_AMT, CURR_AMT, ORIG_DAT, ORIG_CUST_NO
        # FROM {creds.sy_gfc_table}
        # WHERE LST_MAINT_DT > '{self.last_sync}'
        # """

        query = f"""
        SELECT TOP 200 GFC_NO, ORIG_AMT, CURR_AMT, ORIG_DAT, ORIG_CUST_NO
        FROM {creds.sy_gfc_table}
        WHERE LST_MAINT_DT > '{self.last_sync}'
        """

        response = self.db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                if x is not None:
                    result.append(self.Certificate(x, error_handler=self.error_handler))
            return result

    def get_certificates_from_big(self):
        def get_page(page: int):
            response = requests.get(f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates?page={page}&limit=250", headers=creds.test_bc_api_headers)
            return response.json()  

        def get_all_pages():
            page = 1
            response = get_page(page)
            result = []
            while len(response) > 0:
                result += response
                page += 1
                response = get_page(page)
            return result
        
        def get_certificates():
            pages = get_all_pages()
            result = []
            for page in pages:
                for cert in page:
                    result.append(self.BigCommerceCertificate(cert, error_handler=self.error_handler))

            return result
        
        return get_certificates()

    def sync(self):
        # self.big_processor.process()
        self.processor.process()

        self.error_handler.print_errors()
        
    class Certificate:
        def __init__(self, cert_result, error_handler: ErrorHandler = None):
            self.gift_card_no = cert_result[0]
            self.original_amount = cert_result[1]
            self.current_amount = cert_result[2]
            self.original_date = cert_result[3]
            self.cust_no = cert_result[4]
            self.user_info = self.get_user_info()

            self.error_handler: ErrorHandler = error_handler
            self.logger: Logger = self.error_handler.logger

        def get_user_info(self):
            if not self.cust_no:
                return {
                    "name": self.gift_card_no,
                    "email": f"{self.gift_card_no}@store.com"
                }

            query = f"""
            SELECT FST_NAM, LST_NAM, EMAIL_ADRS_1
            FROM {creds.ar_cust_table}
            WHERE CUST_NO = '{self.cust_no}'
            """

            response = Database.db.query_db(query)

            blank_user = {
                "name": f"{self.cust_no}",
                "email": f"{self.cust_no}@store.com"
            }

            if response is not None:
                result = []
                for x in response:
                    if x is not None:
                        result.append({
                            "name": f"{x[0]} {x[1]}",
                            "email": x[2] if (x[2] is not None and x[2] != "") else f"{self.cust_no}@store.com"
                        })

                if len(result) > 0:
                    return result[0]
                else:
                    return blank_user
            else:
                return blank_user

        def sync(self):
            class SQLSync:
                def __init__(self, gift_card_no):
                    self.gift_card_no = gift_card_no
                    self.db = Database.db
                
                def insert(self, bc_id: int):
                    query = f"""
                    INSERT INTO {creds.bc_gift_table}
                    (GFC_NO, BC_GFC_ID)
                    VALUES ('{self.gift_card_no}', {bc_id})
                    """
                    self.db.query_db(query, commit=True)

                def update(self, bc_id: int):
                    query = f"""
                    UPDATE {creds.bc_gift_table}
                    SET BC_GFC_ID = {bc_id}
                    WHERE GFC_NO = '{self.gift_card_no}'
                    """
                    self.db.query_db(query, commit=True)

            return SQLSync(self.gift_card_no)

        def process(self, session: requests.Session):
            def write_payload(bc_id:int = None):
                payload = {
                    "code": self.gift_card_no,
                    "amount": str(self.original_amount),
                    "balance": str(self.current_amount),
                    "purchase_date": utilities.convert_to_rfc2822(self.original_date),
                    "to_name": self.user_info['name'],
                    "to_email": self.user_info['email'],
                    "from_name": self.user_info['name'],
                    "from_email": self.user_info['email'],
                    "status": "active" if self.current_amount > 0 else "disabled"
                }


                if bc_id is not None:
                    payload['id'] = bc_id

                return payload

            def get_bc_id():
                query = f"""
                SELECT BC_GFC_ID
                FROM {creds.bc_gift_table}
                WHERE GFC_NO = '{self.gift_card_no}'
                """

                response = Database.db.query_db(query)
                if response is not None:
                    return response[0][0]
                else:
                    return None

            def create():
                self.logger.info(f"Creating gift certificate {self.gift_card_no}")
                payload = write_payload()
                response = session.post(f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates", json=payload, headers=creds.test_bc_api_headers)

                if response.status_code == 429:
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    self.logger.warn(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                    time.sleep(seconds_to_wait)

                    response = session.post(f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates", json=payload, headers=creds.test_bc_api_headers)

                if response.status_code == 201:
                    self.logger.success(f"Gift certificate {self.gift_card_no} created successfully")
                    self.sync().insert(response.json()['id'])
                else:
                    self.error_handler.add_error_v(f"Error creating gift certificate {self.gift_card_no}")

            def update():
                self.logger.info(f"Updating gift certificate {self.gift_card_no}")

                bc_id = get_bc_id()
                if bc_id is None:
                    self.error_handler.add_error_v(f"Gift certificate {self.gift_card_no} not found.")
                    return

                payload = write_payload()
                response = session.put(f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates/{bc_id}", json=payload, headers=creds.test_bc_api_headers)

                if response.status_code == 429:
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    self.logger.warn(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                    time.sleep(seconds_to_wait)

                    response = session.put(f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates/{bc_id}", json=payload, headers=creds.test_bc_api_headers)

                if response.status_code == 200:
                    self.logger.success(f"Gift certificate {self.gift_card_no} updated successfully")
                    self.sync().update(bc_id)
                else:
                    self.error_handler.add_error_v(f"Error updating gift certificate {self.gift_card_no}")
            
            def get_processing_method():
                bc_id = get_bc_id()
                if bc_id is None:
                    return create()
                else:
                    return update()
                
            get_processing_method()

    class BigCommerceCertificate:
        def __init__(self, cert_result, error_handler: ErrorHandler = None):
            self.gift_card_no = cert_result['code']
            self.original_amount = cert_result['amount']
            self.current_amount = cert_result['balance']
            self.original_date = cert_result['purchase_date']
            self.user_info = {
                "name": cert_result['from_name'],
                "email": cert_result['from_email']
            }
            self.bc_id = cert_result['id']
            self.customer = self.get_customer_from_info(self.user_info)
            self.cust_no = self.customer.cust_no if self.customer is not None else None

            self.error_handler: ErrorHandler = error_handler
            self.logger: Logger = self.error_handler.logger

        class Customer:
            def __init__(self, cust_result):
                self.cust_no = cust_result[0]

        def get_customer_from_info(self, user_info):
            columns = "CUST_NO"

            queries = [f"""
            SELECT {columns} FROM {creds.ar_cust_table} WHERE
            NAM like '{user_info['name']}' and
            EMAIL_ADRS_1 like '{user_info['email']}'
            """]

            if user_info['email'].endswith("@store.com"):
                cust_no = user_info['email'].split("@")[0]

                query = f"""
                SELECT {columns} FROM {creds.ar_cust_table} WHERE
                CUST_NO like '{cust_no}'
                """

                queries.append(query)

            query = f"""
            SELECT {columns} FROM {creds.ar_cust_table} WHERE
            CUST_NO like '{user_info['name']}'
            """

            queries.append(query)

            for query in queries:
                response = Database.db.query_db(query)
                if response is not None:
                    if len(response) > 1:
                        self.logger.warn(f"Multiple customers found for {user_info['name']} {user_info['email']}")
                    elif len(response) == 1:
                        return self.Customer(response[0])
                
            return None

        def sync(self):
            class SQLSync:
                def __init__(self, gift_card_no):
                    self.gift_card_no = gift_card_no
                    self.db = Database.db
                
                def insert(self, bc_id: int):
                    query = f"""
                    INSERT INTO {creds.bc_gift_table}
                    (GFC_NO, BC_GFC_ID)
                    VALUES ('{self.gift_card_no}', {bc_id})
                    """
                    self.db.query_db(query, commit=True)

                def update(self, bc_id: int):
                    query = f"""
                    UPDATE {creds.bc_gift_table}
                    SET BC_GFC_ID = {bc_id}
                    WHERE GFC_NO = '{self.gift_card_no}'
                    """
                    self.db.query_db(query, commit=True)

            return SQLSync(self.gift_card_no)

        def process(self, session: requests.Session):
            pass


import setup.date_presets as date_presets
if __name__ == "__main__":
    certs = GiftCertificates(last_sync=date_presets.business_start_date)
    certs.sync()