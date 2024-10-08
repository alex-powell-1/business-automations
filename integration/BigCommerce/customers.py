import requests

import setup.utilities as utilities
from database import Database
from setup import creds
import setup.date_presets as date_presets
from customer_tools.customers import lookup_customer

import integration.BigCommerce.object_processor as object_processor

from setup.error_handler import ErrorHandler, Logger, ProcessOutErrorHandler

import time
import json

from setup.utilities import VirtualRateLimiter

from integration.shopify_api import Shopify


class Customers:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database.db
        self.logger = ProcessOutErrorHandler.logger
        self.error_handler = ProcessOutErrorHandler.error_handler
        self.update_customer_timestamps()
        self.customers = self.get_cp_customers()
        self.processor = object_processor.ObjectProcessor(objects=self.customers)

    def update_customer_timestamps(self):
        """Update the last maintenance date for all customers in the Middleware who have been updated in
        the AR_LOY_PT_ADJ_HIST table since the last sync."""
        query = f"""SELECT CUST_NO FROM AR_LOY_PT_ADJ_HIST WHERE LST_MAINT_DT > '{self.last_sync}'"""
        response = self.db.query(query)
        customer_list = [x[0] for x in response] if response is not None else []
        if customer_list:
            if len(customer_list) == 1:
                customer_list = f"('{customer_list[0]}')"
            else:
                customer_list = str(tuple(customer_list))

            query = f"""
			UPDATE {creds.Table.CP.Customers.table}
			SET LST_MAINT_DT = GETDATE()
			WHERE CUST_NO IN {customer_list}"""

            response = self.db.query(query)

            if response['code'] == 200:
                self.logger.success('Customer timestamps updated.')
            else:
                self.error_handler.add_error_v(
                    error=f'Error updating customer timestamps.\n\nQuery: {query}\n\nResponse: {response}',
                    origin='update_customer_timestamps',
                )

    def get_cp_customers(self):
        query = f"""
        SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, ADRS_1, CITY, STATE, ZIP_COD, CNTRY
        FROM {creds.Table.CP.Customers.table}
        WHERE
        LST_MAINT_DT > '{self.last_sync}' and
        CUST_NAM_TYP = 'P'
        """
        response = self.db.query(query)
        return [self.Customer(x, self.error_handler) for x in response] if response is not None else []

    def get_bc_customer(self, bc_cust_id):
        url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers?id:in={bc_cust_id}'
        response = requests.get(url, headers=creds.bc_api_headers)
        return response.json()

    def get_mw_customer(self):
        query = f"""
		SELECT BC_CUST_ID FROM {creds.bc_customer_table}
		"""
        response = self.db.query(query)
        if response is not None:
            result = []
            for x in response:
                if x is not None:
                    result.append(x[0])
            return result

    def get_bc_customers(self, mode='customer_id'):
        customer_id_list = []
        page = 1
        more_pages = True
        while more_pages:
            url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers?limit=250&page={page}'
            response = requests.get(url, headers=creds.bc_api_headers)
            for customer in response.json()['data']:
                if mode == 'customer_id':
                    customer_id_list.append(customer['id'])
                if mode == 'all':
                    customer_id_list.append(customer)
            count = response.json()['meta']['pagination']['count']
            if count == 0:
                more_pages = False
            page += 1
        return customer_id_list

    def sync(self):
        if self.customers:
            self.processor.process()

            self.error_handler.print_errors()

    def delete_customers(self, middleware=True):
        """Deletes all customers from BigCommerce and Middleware in 250 count batch."""

        def batch_delete_customers(customer_list):
            while customer_list:
                batch = []
                while len(batch) < 250:
                    if not customer_list:
                        break
                    batch.append(str(customer_list.pop()))

                batch_string = ','.join(batch)
                url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers?id:in={batch_string}'
                bc_response = requests.delete(url=url, headers=creds.bc_api_headers, timeout=120)
                if bc_response.status_code == 204:
                    self.logger.success(f'Customers: {batch_string} deleted from BigCommerce.')
                    query = f"""
                    DELETE FROM {creds.bc_customer_table} WHERE BC_CUST_ID in ({batch_string})
                    """
                    sql_response = Database.query(query)

                    if sql_response['code'] == 200:
                        self.logger.success(f'Customers:\n{batch_string}\nDeleted from Middleware.')
                    else:
                        self.error_handler.add_error_v(
                            error=f'Error deleting customers:\n{batch_string}\nfrom Middleware.'
                        )
                else:
                    self.error_handler.add_error_v(
                        error=f'Error deleting customers:\n{batch_string}\nfrom BigCommerce. Url: {url}'
                    )
                    self.logger.info(f'Response: {json.dumps(bc_response.json(), indent=4)}')

        # Get all customer ids from Middleware
        query = f'SELECT DISTINCT BC_CUST_ID FROM {creds.bc_customer_table}'
        response = Database.query(query)
        customer_id_list = [x[0] for x in response] if response is not None else []

        if customer_id_list:
            batch_delete_customers(customer_list=customer_id_list)
        else:
            self.logger.warn('No customers found in Middleware. Will Check BigCommerce.')

        # Final Cleanup in case of broken mappings
        # Get all customer ids from BigCommerce
        customer_id_list = self.get_bc_customers()
        if customer_id_list:
            batch_delete_customers(customer_list=customer_id_list)
        else:
            self.logger.info('No customers found in BigCommerce.')

    def backfill_middleware(self):
        bc_customers = self.get_bc_customers(mode='all')
        # bc_customers = self.get_bc_customer(bc_cust_id='4645')['data']
        # print(bc_customers)
        mw_customers = self.get_mw_customer()
        if bc_customers:
            total_customers = len(bc_customers)
            print(total_customers)
            for customer in bc_customers:
                bc_cust_id = customer['id']
                if bc_cust_id not in mw_customers:
                    print(f'Processing customer {bc_cust_id}')
                    email = customer['email']
                    phone = customer['phone']

                    cp_cust_id = lookup_customer(email_address=email, phone_number=phone)

                    if cp_cust_id is not None:
                        print(f'Customer found in counterpoint: {email} {phone} {cp_cust_id}')
                        query = f"""
						INSERT INTO {creds.bc_customer_table} (CUST_NO, BC_CUST_ID) VALUES ('{cp_cust_id}', '{bc_cust_id}')"""
                        response = Database.query(query)

                        if response['code'] == 200:
                            self.logger.success(f'Customer {cp_cust_id} added to Middleware.')
                        else:
                            self.error_handler.add_error_v(
                                error=f'Error adding customer {cp_cust_id} to Middleware. \n\nQuery: {query}\n\nResponse: {response}'
                            )
                    else:
                        print(f'Customer not found in counterpoint: {email} {phone}')
                else:
                    print(f'Customer {bc_cust_id} already in Middleware.')

    class Customer:
        def __init__(self, cust_result, error_handler: ErrorHandler):
            self.cust_no = cust_result[0]
            self.db = Database.db
            self.fst_nam = cust_result[1]
            self.lst_nam = cust_result[2]
            self.email = cust_result[3] if cust_result[3] else f'{self.cust_no}@store.com'
            self.phone = cust_result[4]
            self.loyalty_points = cust_result[5]
            self.address: str = cust_result[6]
            self.city = cust_result[7]
            self.state = cust_result[8]
            self.zip = cust_result[9]
            self.country = cust_result[10]

            self.error_handler: ErrorHandler = error_handler
            self.logger: Logger = error_handler.logger

            if self.loyalty_points < 0:
                self.set_loyalty_points_to_zero()
                self.loyalty_points = 0

        def has_phone(self):
            return self.phone is not None or self.phone != ''

        def has_address(self):
            if self.address is not None and self.state is not None and self.zip is not None:
                if self.address.replace(' ', '').isalpha() or self.address.replace(' ', '').isnumeric():
                    self.error_handler.add_error_v(
                        f'Customer {self.cust_no} has malformed address: {self.address}.'
                    )
                    return False

                if self.city is None or self.city == '':
                    self.city = 'CITY'

                if self.country is None or self.country == '':
                    self.country = 'US'

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

                    self.db.query(query)

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

                    self.db.query(query)
                    self.db.query(query2)

                def delete(self):
                    query = f"""
                    DELETE FROM {creds.bc_customer_table}
                    WHERE CUST_NO = '{self.cust_no}'
                    """

                    self.db.query(query)

            return SQLSync(cust_no=self.cust_no)

        def process(self, session: requests.Session):
            def write_customer_payload(bc_cust_id: int = None):
                payload = {}
                if bc_cust_id is not None:
                    payload['id'] = bc_cust_id

                payload['first_name'] = self.fst_nam
                payload['last_name'] = self.lst_nam
                payload['email'] = self.email
                payload['store_credit_amounts'] = [{'amount': self.loyalty_points}]

                if self.phone:
                    payload['phone'] = self.phone

                if self.has_address():

                    def state_code_to_full_name(state_code):
                        states = {
                            'AL': 'Alabama',
                            'AK': 'Alaska',
                            'AZ': 'Arizona',
                            'AR': 'Arkansas',
                            'CA': 'California',
                            'CO': 'Colorado',
                            'CT': 'Connecticut',
                            'DE': 'Delaware',
                            'FL': 'Florida',
                            'GA': 'Georgia',
                            'HI': 'Hawaii',
                            'ID': 'Idaho',
                            'IL': 'Illinois',
                            'IN': 'Indiana',
                            'IA': 'Iowa',
                            'KS': 'Kansas',
                            'KY': 'Kentucky',
                            'LA': 'Louisiana',
                            'ME': 'Maine',
                            'MD': 'Maryland',
                            'MA': 'Massachusetts',
                            'MI': 'Michigan',
                            'MN': 'Minnesota',
                            'MS': 'Mississippi',
                            'MO': 'Missouri',
                            'MT': 'Montana',
                            'NE': 'Nebraska',
                            'NV': 'Nevada',
                            'NH': 'New Hampshire',
                            'NJ': 'New Jersey',
                            'NM': 'New Mexico',
                            'NY': 'New York',
                            'NC': 'North Carolina',
                            'ND': 'North Dakota',
                            'OH': 'Ohio',
                            'OK': 'Oklahoma',
                            'OR': 'Oregon',
                            'PA': 'Pennsylvania',
                            'RI': 'Rhode Island',
                            'SC': 'South Carolina',
                            'SD': 'South Dakota',
                            'TN': 'Tennessee',
                            'TX': 'Texas',
                            'UT': 'Utah',
                            'VT': 'Vermont',
                            'VA': 'Virginia',
                            'WA': 'Washington',
                            'WV': 'West Virginia',
                            'WI': 'Wisconsin',
                            'WY': 'Wyoming',
                        }

                        return states[state_code] if state_code in states else state_code

                    address = {
                        'first_name': self.fst_nam,
                        'last_name': self.lst_nam,
                        'address1': self.address,
                        'city': self.city,
                        'postal_code': self.zip,
                        'state_or_province': (
                            state_code_to_full_name(self.state) if len(self.state) == 2 else self.state
                        ),
                        'country_code': utilities.country_to_country_code(
                            self.country if self.country is not None else 'United States'
                        ),
                    }

                    payload['addresses'] = [address]

                return [payload]

            def create():
                self.logger.info(f'Creating customer {self.cust_no}')
                url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers'
                payload = write_customer_payload()

                response = session.post(url=url, headers=creds.bc_api_headers, json=payload)

                if response.status_code == 429:
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    time.sleep(seconds_to_wait)

                    response = session.post(url=url, headers=creds.bc_api_headers, json=payload)

                if response.status_code == 200:
                    self.logger.success(f'Customer {self.cust_no} created successfully.')
                    self.sync().insert(response.json()['data'][0]['id'])
                else:
                    errors = response.json()['errors']

                    for error in errors:
                        error_msg = errors[error]
                        self.error_handler.add_error_v(error_msg, origin=f'Customer {self.cust_no}')

            def get_bc_id():
                query = f"""
                SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                WHERE CUST_NO = '{self.cust_no}'
                """
                response = self.db.query(query)
                if response is not None:
                    return response[0][0]
                else:
                    return None

            def update():
                self.logger.info(f'Updating customer {self.cust_no}')
                id = get_bc_id()
                if id is None:
                    self.error_handler.add_error_v(
                        f'Customer {self.cust_no} not found in {creds.bc_customer_table}.'
                    )
                else:
                    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers'
                    payload = write_customer_payload(bc_cust_id=id)

                    response = session.put(url=url, headers=creds.bc_api_headers, json=payload)

                    if response.status_code == 429:
                        ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                        seconds_to_wait = (ms_to_wait / 1000) + 1
                        VirtualRateLimiter.pause_requests(seconds_to_wait)
                        time.sleep(seconds_to_wait)

                        response = session.put(url=url, headers=creds.bc_api_headers, json=payload)

                    if response.status_code == 200:
                        self.logger.success(f'Customer {self.cust_no} updated successfully.')
                        self.sync().update(id)
                    else:
                        self.error_handler.add_error_v(f'Error updating customer {self.cust_no}.')

                        errors = response.json()['errors']

                        for error in errors:
                            error_msg = errors[error]
                            self.error_handler.add_error_v(error_msg, origin=f'Customer {self.cust_no}')

            def delete():
                self.logger.info(f'Deleting customer {self.cust_no}')
                id = get_bc_id()
                if id is None:
                    self.error_handler.add_error_v(f'Customer {self.cust_no} not found in database.')
                else:
                    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v3/customers?id:in={id}'
                    response = session.delete(url=url, headers=creds.bc_api_headers)

                    if response.status_code == 429:
                        ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                        seconds_to_wait = (ms_to_wait / 1000) + 1
                        VirtualRateLimiter.pause_requests(seconds_to_wait)
                        time.sleep(seconds_to_wait)

                        response = session.delete(url=url, headers=creds.bc_api_headers)

                    if response.status_code == 204:
                        self.logger.success(f'Customer {self.cust_no} deleted successfully.')
                        self.sync().delete()
                    else:
                        self.error_handler.add_error_v(f'Error deleting customer {self.cust_no}.')

                        errors = response.json()['errors']

                        for error in errors:
                            error_msg = errors[error]
                            self.error_handler.add_error_v(error_msg, origin=f'Customer {self.cust_no}')

            def get_processing_method():
                del_query = f"""
                SELECT CUST_NO FROM {creds.Table.CP.Customers.table}
                WHERE CUST_NO = '{self.cust_no}'
                """

                response = self.db.query(del_query)
                if response is None or len(response) == 0:
                    return 'delete'

                query = f"""
                SELECT BC_CUST_ID FROM {creds.bc_customer_table}
                WHERE CUST_NO = '{self.cust_no}'
                """
                response = self.db.query(query)
                if response is not None:
                    return 'update'
                else:
                    return 'create'

            if get_processing_method() == 'create':
                create()
            elif get_processing_method() == 'update':
                update()
            elif get_processing_method() == 'delete':
                delete()

        def set_loyalty_points_to_zero(self):
            query = f"""
			UPDATE AR_CUST
			SET LOY_PTS_BAL = 0, LST_MAINT_DT = GETDATE()
			WHERE CUST_NO = '{self.cust_no}'
			"""
            response = self.db.query(query)
            if response['code'] == 200:
                self.logger.success(f'Customer {self.cust_no} loyalty points set to 0.')
            else:
                self.error_handler.add_error_v(error=f'Error setting customer {self.cust_no} loyalty points to 0.')


if __name__ == '__main__':
    customers = Customers(last_sync=date_presets.business_start_date)
    # customers.backfill_middleware()
    customers.sync()
