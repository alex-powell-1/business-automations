import requests
from integration.database import Database
from integration.shopify_api import Shopify
from setup import creds
import setup.date_presets as date_presets
from datetime import datetime
import integration.object_processor as object_processor

from setup.error_handler import ProcessOutErrorHandler


class Customers:
    def __init__(self, last_sync=datetime(1970, 1, 1)):
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
            Database.Counterpoint.Customer.update_timestamps(customer_list)

    def get_cp_customers(self):
        response = Database.Counterpoint.Customer.get()
        return [self.Customer(x, self.error_handler) for x in response] if response is not None else []

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

    def sync(self):
        if self.customers:
            self.processor.process()
            self.error_handler.print_errors()

    class Customer:
        def __init__(self, cust_result):
            self.cp_cust_no = cust_result[0]
            self.db = Database.db
            self.fst_nam = cust_result[1]
            self.lst_nam = cust_result[2]
            self.email = cust_result[3] if cust_result[3] else f'{self.cp_cust_no}@store.com'
            self.phone = cust_result[4]
            self.loyalty_points = cust_result[5]
            self.address: str = cust_result[6]
            self.city = cust_result[7]
            self.state = cust_result[8]
            self.zip = cust_result[9]
            self.country = cust_result[10]
            self.shopify_cust_no = cust_result[11]

            self.error_handler = Customers.error_handler
            self.logger = Customers.logger

            if self.loyalty_points < 0:
                self.set_loyalty_points_to_zero()
                self.loyalty_points = 0

        def has_phone(self):
            return self.phone is not None or self.phone != ''

        def has_address(self):
            if self.address is not None and self.state is not None and self.zip is not None:
                if self.address.replace(' ', '').isalpha() or self.address.replace(' ', '').isnumeric():
                    self.error_handler.add_error_v(
                        f'Customer {self.cp_cust_no} has malformed address: {self.address}.'
                    )
                    return False

                if self.city is None or self.city == '':
                    self.city = 'CITY'

                if self.country is None or self.country == '':
                    self.country = 'US'

                return True
            else:
                return False

        def process(self, session: requests.Session):
            def write_customer_payload():
                variables = {
                    'input': {
                        'firstName': self.fst_nam,
                        'lastName': self.lst_nam,
                        'addresses': [],
                        'metafields': [
                            {
                                'namespace': 'counterpoint',
                                'key': 'customer_number',
                                'type': 'single_line_text_field',
                                'value': self.cp_cust_no,
                            }
                        ],
                    }
                }

                # Add optional fields if they are provided
                if self.shopify_cust_no is not None:
                    variables['input']['id'] = f'{Shopify.Customer.prefix}{self.shopify_cust_no}'

                if self.email:
                    variables['input']['email'] = self.email
                    variables['input']['emailMarketingConsent'] = {'marketingState': 'SUBSCRIBED'}

                if self.phone:
                    variables['input']['phone'] = self.phone

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

                if self.addr_lst:
                    for i in self.addr_lst:
                        variables['input']['addresses'].append(
                            {
                                'address1': i['address'],
                                'city': i['city'],
                                'phone': i['phone'],
                                'provinceCode': state_code_to_full_name(i['state']),
                                'zip': i['zip'],
                                'lastName': i['last_name'],
                                'firstName': i['first_name'],
                                'country': i['country'],
                            }
                        )

                return variables

            def create():
                self.logger.info(f'Creating customer {self.cp_cust_no}')
                self.shopify_cust_no = Shopify.Customer.create(write_customer_payload())
                Database.Shopify.Customer.insert(self)

            def update():
                self.logger.info(f'Updating customer {self.cp_cust_no}')
                Shopify.Customer.update(write_customer_payload())
                Database.Shopify.Customer.update(self)

            def delete():
                self.logger.info(f'Deleting customer {self.cp_cust_no}')
                Shopify.Customer.delete(self.shopify_cust_no)
                Database.Shopify.Customer.delete(self)

            del_query = f"""
            SELECT CUST_NO FROM {creds.ar_cust_table}
            WHERE CUST_NO = '{self.cp_cust_no}'
            """

            response = self.db.query(del_query)
            if response is None or len(response) == 0:
                return delete()

            if self.shopify_cust_no is not None:
                update()
            else:
                create()

        def set_loyalty_points_to_zero(self):
            query = f"""
            UPDATE AR_CUST
            SET LOY_PTS_BAL = 0, LST_MAINT_DT = GETDATE()
            WHERE CUST_NO = '{self.cp_cust_no}'
            """
            response = self.db.query(query, commit=True)
            if response['code'] == 200:
                self.logger.success(f'Customer {self.cp_cust_no} loyalty points set to 0.')
            else:
                self.error_handler.add_error_v(
                    error=f'Error setting customer {self.cp_cust_no} loyalty points to 0.'
                )

        def update_loyalty_points(self):
            Shopify.Customer.StoreCredit.update(self.shopify_cust_no, self.loyalty_points)


if __name__ == '__main__':
    customers = Customers(last_sync=date_presets.business_start_date)
    # customers.backfill_middleware()
    customers.sync()
