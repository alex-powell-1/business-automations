import requests

import integration.utilities as utilities
from integration.database import Database
from setup import creds
import setup.date_presets as date_presets

import integration.object_processor as object_processor

from integration.error_handler import ErrorHandler, Logger, GlobalErrorHandler

import time

from integration.utilities import VirtualRateLimiter


class GiftCertificates:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database.db

        self.logger = GlobalErrorHandler.logger
        self.error_handler = GlobalErrorHandler.error_handler

        self.certificates = self.get_certificates()
        self.processor = object_processor.ObjectProcessor(objects=self.certificates)

    def get_certificates(self):
        query = f"""
        SELECT GFC_NO, ORIG_AMT, CURR_AMT, ORIG_DAT, ORIG_CUST_NO
        FROM {creds.sy_gfc_table}
        WHERE LST_MAINT_DT > '{self.last_sync}'
        """

        # query = f"""
        # SELECT TOP 200 GFC_NO, ORIG_AMT, CURR_AMT, ORIG_DAT, ORIG_CUST_NO
        # FROM {creds.sy_gfc_table}
        # WHERE LST_MAINT_DT > '{self.last_sync}'
        # """

        response = self.db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                if x is not None:
                    result.append(self.Certificate(x, error_handler=self.error_handler))
            return result

    def sync(self):
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
                    "email": f"{self.gift_card_no}@store.com",
                }

            query = f"""
            SELECT FST_NAM, LST_NAM, EMAIL_ADRS_1
            FROM {creds.ar_cust_table}
            WHERE CUST_NO = '{self.cust_no}'
            """

            response = Database.db.query_db(query)

            blank_user = {
                "name": f"{self.cust_no}",
                "email": f"{self.cust_no}@store.com",
            }

            if response is not None:
                result = []
                for x in response:
                    if x is not None:
                        result.append(
                            {
                                "name": f"{x[0]} {x[1]}",
                                "email": (
                                    x[2]
                                    if (x[2] is not None and x[2] != "")
                                    else f"{self.cust_no}@store.com"
                                ),
                            }
                        )

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
            def write_payload(bc_id: int = None):
                payload = {
                    "code": self.gift_card_no,
                    "amount": str(self.original_amount),
                    "balance": str(self.current_amount),
                    "purchase_date": utilities.convert_to_rfc2822(self.original_date),
                    "to_name": self.user_info["name"],
                    "to_email": self.user_info["email"],
                    "from_name": self.user_info["name"],
                    "from_email": self.user_info["email"],
                    "status": "active" if self.current_amount > 0 else "disabled",
                }

                if bc_id is not None:
                    payload["id"] = bc_id

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
                response = session.post(
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates",
                    json=payload,
                    headers=creds.test_bc_api_headers,
                )

                if response.status_code == 429:
                    ms_to_wait = int(response.headers["X-Rate-Limit-Time-Reset-Ms"])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    time.sleep(seconds_to_wait)

                    response = session.post(
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates",
                        json=payload,
                        headers=creds.test_bc_api_headers,
                    )

                if response.status_code == 201:
                    self.logger.success(
                        f"Gift certificate {self.gift_card_no} created successfully"
                    )
                    self.sync().insert(response.json()["id"])
                else:
                    self.error_handler.add_error_v(
                        f"Error creating gift certificate {self.gift_card_no}"
                    )

            def update():
                self.logger.info(f"Updating gift certificate {self.gift_card_no}")

                bc_id = get_bc_id()

                if bc_id is None:
                    self.error_handler.add_error_v(
                        f"Gift certificate {self.gift_card_no} not found."
                    )
                    return

                payload = write_payload()
                response = session.put(
                    f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates/{bc_id}",
                    json=payload,
                    headers=creds.test_bc_api_headers,
                )

                if response.status_code == 429:
                    ms_to_wait = int(response.headers["X-Rate-Limit-Time-Reset-Ms"])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    time.sleep(seconds_to_wait)

                    response = session.put(
                        f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v2/gift_certificates/{bc_id}",
                        json=payload,
                        headers=creds.test_bc_api_headers,
                    )

                if response.status_code == 200:
                    self.logger.success(
                        f"Gift certificate {self.gift_card_no} updated successfully"
                    )
                    self.sync().update(bc_id)
                else:
                    self.error_handler.add_error_v(
                        f"Error updating gift certificate {self.gift_card_no}"
                    )

            def get_processing_method():
                bc_id = get_bc_id()
                if bc_id is None:
                    return create()
                else:
                    return update()

            get_processing_method()


if __name__ == "__main__":
    certs = GiftCertificates(last_sync=date_presets.business_start_date)
    certs.sync()
