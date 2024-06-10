import requests

import integration.utilities as utilities
from integration.database import Database
from setup import creds

import integration.object_processor as object_processor

from integration.error_handler import ErrorHandler, Logger

import time

class Customers:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database.db
        self.customers = self.get_customers()
        self.processor = object_processor.ObjectProcessor(objects=self.customers, speed=50)

        self.logger = Logger(log_file="logs/customers.log")
        self.error_handler = ErrorHandler(logger=self.logger)

    def get_customers(self):
        query = f"""
        SELECT TOP  CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, ADRS_1, CITY, STATE, ZIP_COD, CNTRY
        FROM {creds.ar_cust_table}
        WHERE
        LST_MAINT_DT > '{self.last_sync}' and
        CUST_NAM_TYP = 'P'
        """
        
        response = self.db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                if x is not None:
                    result.append(self.Customer(x))
            return result

    def sync(self):
        self.processor.process()

        self.error_handler.print_errors()

    class Customer:
        def __init__(self, cust_result):
            self.cust_no = cust_result[0]
            self.db = Database.db
            self.fst_nam = cust_result[1]
            self.lst_nam = cust_result[2]
            self.email = cust_result[3] if cust_result[3] else f"{self.cust_no}@store.com"
            self.phone = cust_result[4]
            self.loyalty_points = cust_result[5]
            self.address: str = cust_result[6]
            self.city = cust_result[7]
            self.state = cust_result[8]
            self.zip = cust_result[9]
            self.country = cust_result[10]

        def has_phone(self):
            return self.phone is not None or self.phone != ""

        def has_address(self):
            if self.address is not None and self.state is not None and self.zip is not None:
                if self.address.replace(" ", "").isalpha() or self.address.replace(" ", "").isnumeric():
                    print(f"Customer {self.cust_no} has malformed address: {self.address}.")
                    self.error_handler.add_error(f"Customer {self.cust_no} has malformed address: {self.address}.")
                    return False

                if self.city is None or self.city == "":
                    self.city = "CITY"

                if self.country is None or self.country == "":
                    self.country = "US"

                return True
            else:
                return False

        def sync(self):
            class SQLSync:
                def __init__(self, cust_no):
                    self.db = Database.db
                    self.cust_no = cust_no

                def insert(self, bc_cust_id: int):
                    query = f"""
                    INSERT INTO {creds.bc_customer_table}
                    (CUST_NO, BC_CUST_ID)
                    VALUES ('{self.cust_no}', {bc_cust_id})
                    """

                    self.db.query_db(query, commit=True)

                def update(self, bc_cust_id: int):
                    query = f"""
                    UPDATE {creds.bc_customer_table}
                    SET BC_CUST_ID = {bc_cust_id}
                    WHERE CUST_NO = '{self.cust_no}'
                    """

                    query2 = f"""
                    UPDATE {creds.bc_customer_table}
                    SET LST_MAINT_DT = GETDATE()
                    WHERE CUST_NO = '{self.cust_no}'
                    """

                    self.db.query_db(query, commit=True)
                    self.db.query_db(query2, commit=True)

                def delete(self):
                    query = f"""
                    DELETE FROM {creds.bc_customer_table}
                    WHERE CUST_NO = '{self.cust_no}'
                    """

                    self.db.query_db(query, commit=True)

            return SQLSync(cust_no=self.cust_no)

        def process(self, session: requests.Session):
            def write_customer_payload(bc_cust_id: int = None):
                payload = {}
                if bc_cust_id is not None:
                    payload["id"] = bc_cust_id

                payload["first_name"] = self.fst_nam
                payload["last_name"] = self.lst_nam
                payload["email"] = self.email
                payload["store_credit_amounts"] = [{"amount": self.loyalty_points}]

                if self.has_phone():
                    payload["phone"] = self.phone

                if self.has_address():
                    def state_code_to_full_name(state_code):
                        states = {
                            "AL": "Alabama",
                            "AK": "Alaska",
                            "AZ": "Arizona",
                            "AR": "Arkansas",
                            "CA": "California",
                            "CO": "Colorado",
                            "CT": "Connecticut",
                            "DE": "Delaware",
                            "FL": "Florida",
                            "GA": "Georgia",
                            "HI": "Hawaii",
                            "ID": "Idaho",
                            "IL": "Illinois",
                            "IN": "Indiana",
                            "IA": "Iowa",
                            "KS": "Kansas",
                            "KY": "Kentucky",
                            "LA": "Louisiana",
                            "ME": "Maine",
                            "MD": "Maryland",
                            "MA": "Massachusetts",
                            "MI": "Michigan",
                            "MN": "Minnesota",
                            "MS": "Mississippi",
                            "MO": "Missouri",
                            "MT": "Montana",
                            "NE": "Nebraska",
                            "NV": "Nevada",
                            "NH": "New Hampshire",
                            "NJ": "New Jersey",
                            "NM": "New Mexico",
                            "NY": "New York",
                            "NC": "North Carolina",
                            "ND": "North Dakota",
                            "OH": "Ohio",
                            "OK": "Oklahoma",
                            "OR": "Oregon",
                            "PA": "Pennsylvania",
                            "RI": "Rhode Island",
                            "SC": "South Carolina",
                            "SD": "South Dakota",
                            "TN": "Tennessee",
                            "TX": "Texas",
                            "UT": "Utah",
                            "VT": "Vermont",
                            "VA": "Virginia",
                            "WA": "Washington",
                            "WV": "West Virginia",
                            "WI": "Wisconsin",
                            "WY": "Wyoming"
                        }

                        return states[state_code] if state_code in states else state_code

                    address = {
                        "first_name": self.fst_nam,
                        "last_name": self.lst_nam,
                        "address1": self.address,
                        "city": self.city,
                        "postal_code": self.zip,
                        "state_or_province": state_code_to_full_name(self.state) if len(self.state) == 2 else self.state,
                        "country_code": utilities.country_to_country_code(
                            self.country if self.country is not None else "United States")
                    }

                    payload["addresses"] = [address]

                return [payload]

            def create():
                print(f"Creating customer {self.cust_no}")
                url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers"
                payload = write_customer_payload()

                response = session.post(url=url, headers=creds.test_bc_api_headers, json=payload)

                if response.status_code == 429:
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    print(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                    time.sleep(seconds_to_wait)

                    response = session.post(url=url, headers=creds.test_bc_api_headers, json=payload)

                if response.status_code == 200:
                    print(f"Customer {self.cust_no} created successfully.")
                    self.sync().insert(response.json()["data"][0]["id"])
                else:
                    print(f"Error creating customer {self.cust_no}.")
                    self.error_handler.add_error(f"Error creating customer {self.cust_no}.")

                    errors = response.json()["errors"]

                    for error in errors:
                        error_msg = errors[error]
                        self.error_handler.add_error(error_msg, origin=f"Customer {self.cust_no}", type="BigCommerce API")

            def get_bc_id():
                query = f"""
                SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                WHERE CUST_NO = '{self.cust_no}'
                """
                response = self.db.query_db(query)
                if response is not None:
                    return response[0][0]
                else:
                    return None

            def update():
                id = get_bc_id()
                if id is None:
                    print(f"Customer {self.cust_no} not found in database.")
                else:
                    print(f"Updating customer {self.cust_no}")
                    url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers"
                    payload = write_customer_payload(bc_cust_id=id)

                    response = session.put(url=url, headers=creds.test_bc_api_headers, json=payload)

                    if response.status_code == 429:
                        ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                        seconds_to_wait = (ms_to_wait / 1000) + 1
                        print(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                        time.sleep(seconds_to_wait)

                    response = session.put(url=url, headers=creds.test_bc_api_headers, json=payload)

                    if response.status_code == 200:
                        print(f"Customer {self.cust_no} updated successfully.")
                        self.sync().update(id)
                    else:
                        print(f"Error updating customer {self.cust_no}.")
                        self.error_handler.add_error(f"Error updating customer {self.cust_no}.")

                        errors = response.json()["errors"]

                        for error in errors:
                            error_msg = errors[error]
                            self.error_handler.add_error(error_msg, origin=f"Customer {self.cust_no}", type="BigCommerce API")

            def delete():
                id = get_bc_id()
                if id is None:
                    print(f"Customer {self.cust_no} not found in database.")
                else:
                    print(f"Deleting customer {self.cust_no}")
                    url = f"https://api.bigcommerce.com/stores/{creds.test_big_store_hash}/v3/customers?id:in={id}"
                    response = session.delete(url=url, headers=creds.test_bc_api_headers)

                    if response.status_code == 429:
                        ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                        seconds_to_wait = (ms_to_wait / 1000) + 1
                        print(f"Rate limit exceeded. Waiting {seconds_to_wait} seconds.")
                        time.sleep(seconds_to_wait)

                    response = session.delete(url=url, headers=creds.test_bc_api_headers)

                    if response.status_code == 204:
                        print(f"Customer {self.cust_no} deleted successfully.")
                        self.sync().delete()
                    else:
                        print(f"Error deleting customer {self.cust_no}.")

                        self.error_handler.add_error(f"Error deleting customer {self.cust_no}.")

                        errors = response.json()["errors"]

                        for error in errors:
                            error_msg = errors[error]
                            self.error_handler.add_error(error_msg, origin=f"Customer {self.cust_no}", type="BigCommerce API")

            def get_processing_method():
                del_query = f"""
                SELECT CUST_NO FROM {creds.ar_cust_table}
                WHERE CUST_NO = '{self.cust_no}'
                """

                response = self.db.query_db(del_query)
                if response is None or len(response) == 0:
                    return "delete"

                query = f"""
                SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                WHERE CUST_NO = '{self.cust_no}'
                """
                response = self.db.query_db(query)
                if response is not None:
                    return "update"
                else:
                    return "create"

            if get_processing_method() == "create":
                create()
            elif get_processing_method() == "update":
                update()
            elif get_processing_method() == "delete":
                delete()



import setup.date_presets as date_presets
if __name__ == "__main__":
    customers = Customers(last_sync=date_presets.business_start_date)
    customers.sync()