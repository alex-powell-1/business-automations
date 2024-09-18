from database import Database
import concurrent.futures
from integration.shopify_api import Shopify
from setup.creds import Table
from setup import creds
from datetime import datetime
import json
from traceback import format_exc as tb


from setup.error_handler import ProcessOutErrorHandler


class Customers:
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler

    def __init__(self, last_sync=datetime(1970, 1, 1), test_mode=False, test_customer=None):
        self.last_sync = last_sync
        self.test_mode = test_mode
        self.test_customer = test_customer
        self.update_customer_timestamps()
        self.customers = self.get_updated_customers()

    def update_customer_timestamps(self):
        """Update the last maintenance date for all customers in the Middleware who have been updated in
        the AR_LOY_PT_ADJ_HIST table since the last sync."""

        query = f"""SELECT CUST_NO FROM AR_LOY_PT_ADJ_HIST WHERE LST_MAINT_DT > '{self.last_sync}'"""
        response = Database.query(query)
        customer_list = [x[0] for x in response] if response is not None else []
        if customer_list:
            Database.Counterpoint.Customer.update_timestamps(customer_list)

    def get_updated_customers(self):
        if self.test_mode:
            response = Database.Counterpoint.Customer.get_all(customer_no=self.test_customer)
        else:
            response = Database.Counterpoint.Customer.get_all(last_sync=self.last_sync)

        return [self.Customer(x) for x in response] if response is not None else []

    def get_cp_customers(self):
        """Get all customers from Counterpoint that are e-commerce customers."""
        query = f"""
        SELECT CUST_NO FROM {Table.CP.Customers.table}
        WHERE IS_ECOMM_CUST = 'Y'
        """
        response = Database.query(query)
        return [x[0] for x in response] if response is not None else []

    def get_mw_customers(self):
        """Get all customers from the Middleware. Exclude entries that have a null CUST_NO.
        These are newsletter subscribers that have never made a purchase."""
        query = f"""
        SELECT CUST_NO FROM {Table.Middleware.customers}
        WHERE CUST_NO IS NOT NULL
        """
        response = Database.query(query)
        return [x[0] for x in response] if response is not None else []

    def process_deletes(self):
        Customers.logger.header('Processing Deletes')
        cp_customers = self.get_cp_customers()
        mw_customers = self.get_mw_customers()
        # Find Customers in MW that are not in CP
        delete_queue = []
        for mw_customer in mw_customers:
            if mw_customer not in cp_customers:
                delete_queue.append(mw_customer)

        if delete_queue:
            count = 1
            for x in delete_queue:
                Customers.logger.info(f'{count}/{len(delete_queue)}: Deleting customer CUST_NO: {x}')
                shopify_cust_no = Database.Shopify.Customer.get_id(x)
                Shopify.Customer.delete(shopify_cust_no)
                Database.Shopify.Customer.delete(shopify_cust_no)
                count += 1
        else:
            Customers.logger.info('No customers to delete.')

    def sync(self):
        self.process_deletes()

        if self.customers:
            success_count = 0
            fail_count = {'number': 0, 'customer': []}
            queue_size = len(self.customers)
            Customers.logger.header(f'Syncing Customers: {queue_size}')
            for customer in self.customers:
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
                        executor.submit(customer.process())
                except Exception as e:
                    self.error_handler.add_error_v(
                        error=f'Error processing customer {customer.cp_cust_no}: {e}',
                        origin='Customers.sync',
                        traceback=tb(),
                    )
                    fail_count['number'] += 1
                    fail_count['customer'].append(customer.cp_cust_no)
                else:
                    success_count += 1
                finally:
                    queue_size -= 1

            if fail_count['number'] > 0:
                Customers.logger.warn(f'Customers failed to sync: {fail_count["number"]}')
                Customers.logger.warn(f'Failed Customers: {fail_count["customer"]}')
            if success_count > 0:
                Customers.logger.success(f'Customers synced: {success_count}')

        else:
            Customers.logger.warn('No customers to sync.')

    class Customer:
        def __init__(self, cust_result):
            self.cp_cust_no = cust_result[0]
            self.fst_nam = str(cust_result[1]).title()
            self.lst_nam = str(cust_result[2]).title()
            self.email = cust_result[3]
            self.phone = cust_result[4]
            self.loyalty_points = float(cust_result[5])
            self.loyalty_point_id = cust_result[6]
            self.address_line_1: str = cust_result[7]
            self.address_line_2: str = cust_result[8]
            self.city = cust_result[9]
            self.state = cust_result[10]
            self.zip = cust_result[11]
            self.country = cust_result[12]
            self.shopify_cust_no = cust_result[13]
            self.meta_cust_no_id = cust_result[14]
            self.category = cust_result[15]
            self.meta_category_id = cust_result[16]

            try:
                self.meta_birth_month = int(cust_result[17])
            except:
                self.meta_birth_month = None
            self.meta_birth_month_id = cust_result[18]

            try:
                self.meta_spouse_birth_month = int(cust_result[19])
            except:
                self.meta_spouse_birth_month = None

            self.meta_spouse_birth_month_id = cust_result[20]

            self.meta_wholesale_price_tier = self.validate_price_tier(cust_result[21])

            self.meta_wholesale_price_tier_id = cust_result[22]

            self.sms_subscribe = True if cust_result[23] == 'Y' else False
            self.email_subscribe = True if cust_result[24] == 'Y' else False
            self.mw_id = cust_result[25]

            self.addresses = []
            self.get_addresses()

            if self.loyalty_points < 0:
                self.set_loyalty_points_to_zero()
                self.loyalty_points = 0

        def __str__(self):
            result = f'Customer {self.cp_cust_no}\n'
            result += f'First Name: {self.fst_nam}\n'
            result += f'Last Name: {self.lst_nam}\n'
            result += f'Email: {self.email}\n'
            result += f'Phone: {self.phone}\n'
            result += f'Loyalty Points: {self.loyalty_points}\n'
            result += f'Loyalty Point ID: {self.loyalty_point_id}\n'
            result += f'Address Line 1: {self.address_line_1}\n'
            result += f'Address Line 2: {self.address_line_2}\n'
            result += f'City: {self.city}\n'
            result += f'State: {self.state}\n'
            result += f'Zip: {self.zip}\n'
            result += f'Country: {self.country}\n'
            result += f'Shopify Customer Number: {self.shopify_cust_no}\n'
            result += f'Meta Customer Number ID: {self.meta_cust_no_id}\n'
            result += f'Category: {self.category}\n'
            result += f'Meta Category ID: {self.meta_category_id}\n'
            result += f'Meta Birth Month: {self.meta_birth_month}\n'
            result += f'Meta Birth Month ID: {self.meta_birth_month_id}\n'
            result += f'Meta Spouse Birth Month: {self.meta_spouse_birth_month}\n'
            result += f'Meta Spouse Birth Month ID: {self.meta_spouse_birth_month_id}\n'
            result += f'Meta Wholesale Price Tier: {self.meta_wholesale_price_tier}\n'
            result += f'Meta Wholesale Price Tier ID: {self.meta_wholesale_price_tier_id}\n'
            result += f'SMS Subscribe: {self.sms_subscribe}\n'
            result += f'Middleware ID: {self.mw_id}\n'
            return result

        def validate_price_tier(self, price_tier):
            # Check if price tier is an integer between 0-5
            try:
                price_tier = int(price_tier)
                if price_tier < 0 or price_tier > 5:
                    return None
                return price_tier
            except:
                return None

        def get_addresses(self):
            # Add the primary address
            address_main = {
                'first_name': self.fst_nam,
                'last_name': self.lst_nam,
                'address1': self.address_line_1,
                'address2': self.address_line_2,
                'city': self.city,
                'state': self.state,
                'zip': self.zip,
                'country': self.country,
                'phone': self.phone,
            }
            self.addresses.append(address_main)

            # Get additional addresses
            address_res = Database.Counterpoint.Customer.ShippingAddress.get(cust_no=self.cp_cust_no)
            if address_res is not None:
                for x in address_res:
                    address = {
                        'first_name': x[0],
                        'last_name': x[1],
                        'address1': x[2],
                        'address2': x[3],
                        'city': x[4],
                        'state': x[5],
                        'zip': x[6],
                        'country': x[7],
                        'phone': x[8],
                    }

                    self.addresses.append(address)

        def write_customer_payload(self):
            variables = {
                'input': {'firstName': self.fst_nam, 'lastName': self.lst_nam, 'addresses': [], 'metafields': []}
            }

            # Add optional fields if they are provided
            if self.shopify_cust_no:
                variables['input']['id'] = f'{Shopify.Customer.prefix}{self.shopify_cust_no}'

            if self.email:
                variables['input']['email'] = self.email
                if not self.shopify_cust_no:
                    # Only add email marketing consent if the customer is new
                    # Existing customers will have their email marketing consent updated
                    # customerEmailMarketingConsentUpdate Mutation instead
                    variables['input']['emailMarketingConsent'] = (
                        {'marketingState': 'SUBSCRIBED'}
                        if self.email_subscribe
                        else {'marketingState': 'NOT_SUBSCRIBED'}
                    )
            else:
                variables['input']['email'] = None

            if self.phone:
                variables['input']['phone'] = self.phone
                if not self.shopify_cust_no:
                    # Only add SMS marketing consent if the customer is new
                    # Existing customers will have their SMS marketing consent updated
                    # customerSmsMarketingConsentUpdate Mutation instead

                    variables['input']['smsMarketingConsent'] = (
                        {'marketingState': 'SUBSCRIBED', 'marketingOptInLevel': 'UNKNOWN'}
                        if self.sms_subscribe
                        else {'marketingState': 'NOT_SUBSCRIBED'}
                    )
            else:
                variables['input']['phone'] = None

            ############################################################################################
            ######################################## METAFIELDS ########################################
            ############################################################################################

            namespace = creds.Shopify.Metafield.Namespace.Customer.customer

            # Add Customer Number
            if not self.meta_cust_no_id:
                variables['input']['metafields'].append(
                    {
                        'namespace': namespace,
                        'key': 'number',
                        'type': 'single_line_text_field',
                        'value': self.cp_cust_no,
                    }
                )
            else:
                variables['input']['metafields'].append(
                    {'id': f'gid://shopify/Metafield/{self.meta_cust_no_id}', 'value': self.cp_cust_no}
                )

            # Add Category
            if self.category:
                if not self.meta_category_id:
                    variables['input']['metafields'].append(
                        {
                            'namespace': namespace,
                            'key': 'category',
                            'type': 'single_line_text_field',
                            'value': self.category,
                        }
                    )
                else:
                    variables['input']['metafields'].append(
                        {'id': f'gid://shopify/Metafield/{self.meta_category_id}', 'value': self.category}
                    )
            elif self.meta_category_id:
                Shopify.Metafield.delete(metafield_id=self.meta_category_id)
                self.meta_category_id = None

            # Add Birth Month
            if self.meta_birth_month:
                if not self.meta_birth_month_id:
                    variables['input']['metafields'].append(
                        {
                            'namespace': namespace,
                            'key': 'birth_month',
                            'type': 'number_integer',
                            'value': json.dumps(self.meta_birth_month),
                        }
                    )
                else:
                    variables['input']['metafields'].append(
                        {
                            'id': f'gid://shopify/Metafield/{self.meta_birth_month_id}',
                            'value': json.dumps(self.meta_birth_month),
                        }
                    )
            elif self.meta_birth_month_id:
                Shopify.Metafield.delete(metafield_id=self.meta_birth_month_id)
                self.meta_birth_month_id = None

            # Add Spouse Birth Month
            if self.meta_spouse_birth_month:
                if not self.meta_spouse_birth_month_id:
                    variables['input']['metafields'].append(
                        {
                            'namespace': namespace,
                            'key': 'birth_month_spouse',
                            'type': 'number_integer',
                            'value': json.dumps(self.meta_spouse_birth_month),
                        }
                    )
                else:
                    variables['input']['metafields'].append(
                        {
                            'id': f'gid://shopify/Metafield/{self.meta_spouse_birth_month_id}',
                            'value': json.dumps(self.meta_spouse_birth_month),
                        }
                    )
            elif self.meta_spouse_birth_month_id:
                Shopify.Metafield.delete(metafield_id=self.meta_spouse_birth_month_id)
                self.meta_spouse_birth_month_id = None

            # Add Wholesale Price Tier
            if self.meta_wholesale_price_tier:
                if not self.meta_wholesale_price_tier_id:
                    variables['input']['metafields'].append(
                        {
                            'namespace': namespace,
                            'key': 'wholesale_price_tier',
                            'type': 'number_integer',
                            'value': json.dumps(self.meta_wholesale_price_tier),
                        }
                    )
                else:
                    variables['input']['metafields'].append(
                        {
                            'id': f'gid://shopify/Metafield/{self.meta_wholesale_price_tier_id}',
                            'value': json.dumps(self.meta_wholesale_price_tier),
                        }
                    )
            elif self.meta_wholesale_price_tier_id:
                Shopify.Metafield.delete(metafield_id=self.meta_wholesale_price_tier_id)
                self.meta_wholesale_price_tier_id = None

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

            if self.addresses:
                for i in self.addresses:
                    address = {}
                    if i['first_name']:
                        address['firstName'] = i['first_name']
                    if i['last_name']:
                        address['lastName'] = i['last_name']
                    if i['phone']:
                        address['phone'] = i['phone']
                    if i['address1']:
                        address['address1'] = i['address1']
                    else:
                        # Customers.logger.warn(f'Customer {self.cp_cust_no} has no address1.')
                        continue
                    if i['address2']:
                        address['address2'] = i['address2']
                    if i['city']:
                        address['city'] = i['city']
                    else:
                        # Customers.logger.warn(f'Customer {self.cp_cust_no} has no city.')
                        continue
                    if i['state']:
                        address['provinceCode'] = state_code_to_full_name(i['state'])

                    if i['zip']:
                        address['zip'] = i['zip']

                    if i['country']:
                        address['country'] = i['country']
                    else:
                        address['country'] = 'United States'

                    variables['input']['addresses'].append(address)

            return variables

        def check_for_existing_customer(self):
            """Check for an existing customer in Shopify by email or phone number."""
            shopify_cust_id = None

            if self.email:
                shopify_cust_id = Shopify.Customer.get_by_email(self.email)

            if not shopify_cust_id:
                if self.phone:
                    shopify_cust_id = Shopify.Customer.get_by_phone(self.phone)
                else:
                    shopify_cust_id = None
            if shopify_cust_id:
                self.shopify_cust_no = shopify_cust_id

        def process(self):
            def create():
                Customers.logger.info(f'Creating customer {self.cp_cust_no}')
                return Shopify.Customer.create(self.write_customer_payload())

            def update():
                Customers.logger.info(f'Updating customer {self.cp_cust_no}')
                return Shopify.Customer.update(self.write_customer_payload())

            if not self.shopify_cust_no:
                self.check_for_existing_customer()

            if self.shopify_cust_no:
                response = update()
                if self.phone:
                    Shopify.Customer.update_sms_marketing_consent(self.shopify_cust_no, self.sms_subscribe)
                if self.email:
                    Shopify.Customer.update_email_marketing_consent(self.shopify_cust_no, self.email_subscribe)
            else:
                response = create()

            self.get_ids(response)
            self.update_loyalty_points()

            Database.Shopify.Customer.sync(self)

        def get_ids(self, response):
            self.shopify_cust_no = response['id']

            self.meta_cust_no_id = (
                response['metafields']['cust_no_id'] if 'cust_no_id' in response['metafields'] else None
            )

            self.meta_category_id = (
                response['metafields']['category_id'] if 'category_id' in response['metafields'] else None
            )

            self.meta_birth_month_id = (
                response['metafields']['birth_month_id'] if 'birth_month_id' in response['metafields'] else None
            )

            self.meta_spouse_birth_month_id = (
                response['metafields']['birth_month_spouse_id']
                if 'birth_month_spouse_id' in response['metafields']
                else None
            )

            self.meta_wholesale_price_tier_id = (
                response['metafields']['wholesale_price_tier_id']
                if 'wholesale_price_tier_id' in response['metafields']
                else None
            )

        def update_loyalty_points(self):
            if self.loyalty_point_id is None:
                if self.loyalty_points > 0:
                    self.loyalty_point_id = Shopify.Customer.StoreCredit.add_store_credit(
                        self.shopify_cust_no, self.loyalty_points
                    )
                    return
                else:
                    return

            shopify_loy_bal = Shopify.Customer.StoreCredit.get(self.loyalty_point_id)
            if shopify_loy_bal != self.loyalty_points:
                if shopify_loy_bal < self.loyalty_points:
                    Shopify.Customer.StoreCredit.add_store_credit(
                        self.shopify_cust_no, self.loyalty_points - shopify_loy_bal
                    )
                else:
                    Shopify.Customer.StoreCredit.remove_store_credit(
                        self.shopify_cust_no, shopify_loy_bal - self.loyalty_points
                    )

        def set_loyalty_points_to_zero(self):
            query = f"""
            UPDATE AR_CUST
            SET LOY_PTS_BAL = 0, LST_MAINT_DT = GETDATE()
            WHERE CUST_NO = '{self.cp_cust_no}'
            """
            response = Database.query(query)
            if response['code'] == 200:
                Customers.logger.success(f'Customer {self.cp_cust_no} loyalty points set to 0.')
            else:
                Customers.error_handler.add_error_v(
                    error=f'Error setting customer {self.cp_cust_no} loyalty points to 0.'
                )

        @staticmethod
        def get_metafields(cust_id):
            return Shopify.Customer.Metafield.get(shopify_cust_no=cust_id)['customer']['metafields']['edges']

        @staticmethod
        def get_metafield_keys(cust_id):
            return [x['node']['key'] for x in Customers.Customer.get_metafields(cust_id)]

        @staticmethod
        def has_metafield(cust_id, key):
            return key in Customers.Customer.get_metafield_keys(cust_id)


class Subscriber:
    def __init__(self, email, enabled, create_date):
        self.email = email
        self.enabled = True if enabled == 1 else False
        self.create_date = create_date
        self.shopify_cust_no = None
        self.cp_cust_no = None
        self.mw_id = None
        self.loyalty_point_id = None
        self.meta_cust_no_id = None
        self.meta_category_id = None
        self.meta_birth_month_id = None
        self.meta_spouse_birth_month_id = None
        self.meta_wholesale_price_tier_id = None

    def __str__(self):
        return f'{self.email} - {self.create_date} - Enabled: {self.enabled}'

    def get_mw_id(self):
        query = f"""
        SELECT ID FROM {Table.Middleware.customers}
        WHERE SHOP_CUST_ID = {self.shopify_cust_no}
        """
        response = Database.query(query)
        return response[0][0] if response is not None else None

    def sync(self):
        if self.mw_id:
            Database.Shopify.Customer.update(
                cp_cust_no=self.cp_cust_no,
                shopify_cust_no=self.shopify_cust_no,
                loyalty_point_id=self.loyalty_point_id,
                meta_cust_no_id=self.meta_cust_no_id,
                meta_category_id=self.meta_category_id,
                meta_birth_month_id=self.meta_birth_month_id,
                meta_spouse_birth_month_id=self.meta_spouse_birth_month_id,
                meta_wholesale_price_tier_id=self.meta_wholesale_price_tier_id,
            )
        else:
            Database.Shopify.Customer.insert(
                cp_cust_no=self.cp_cust_no,
                shopify_cust_no=self.shopify_cust_no,
                loyalty_point_id=self.loyalty_point_id,
                meta_cust_no_id=self.meta_cust_no_id,
                meta_category_id=self.meta_category_id,
                meta_birth_month_id=self.meta_birth_month_id,
                meta_spouse_birth_month_id=self.meta_spouse_birth_month_id,
                meta_wholesale_price_tier_id=self.meta_wholesale_price_tier_id,
            )

    def process(self):
        if Database.Counterpoint.Customer.get_by_email(self.email):
            # Customer already exists in Counterpoint. Delete from SN_NEWS
            query = f"""
            DELETE FROM SN_NEWS
            WHERE EMAIL = '{self.email}'
            """
            response = Database.query(query)
            if response['code'] == 200:
                Customers.logger.info(f'Deleted subscriber {self.email}')
            return

        # Check if customer exists in Shopify. If exists, update shopify_cust_no
        shopify_cust_id = Shopify.Customer.get_by_email(self.email)
        if shopify_cust_id:
            self.shopify_cust_no = shopify_cust_id
            Shopify.Customer.update_email_marketing_consent(self.shopify_cust_no, self.enabled)

        else:
            variables = {'input': {'email': self.email, 'emailMarketingConsent': {}, 'metafields': []}}
            variables['input']['emailMarketingConsent'] = (
                {'marketingState': 'SUBSCRIBED'} if self.enabled else {'marketingState': 'NOT_SUBSCRIBED'}
            )

            # Add Category
            variables['input']['metafields'].append(
                {
                    'namespace': creds.Shopify.Metafield.Namespace.Customer.customer,
                    'key': 'category',
                    'type': 'single_line_text_field',
                    'value': 'RETAIL',
                }
            )

            response = Shopify.Customer.create(variables)
            if response:
                self.shopify_cust_no = response['id']
                self.meta_category_id = response['metafields']['category_id']

        self.mw_id = self.get_mw_id()
        self.sync()


class NewsletterSubscribers:
    def __init__(self, last_sync=datetime(1970, 1, 1)):
        self.last_sync = last_sync
        self.subscribers: list[Subscriber] = []

    def sync(self):
        query = f"""
        SELECT EMAIL, ENABLED, CREATED_DT
        FROM {Table.newsletter}
        WHERE CREATED_DT > '{self.last_sync}'
        """
        response = Database.query(query)
        self.subscribers = [Subscriber(x[0], x[1], x[2]) for x in response] if response is not None else []
        if self.subscribers:
            for subscriber in self.subscribers:
                try:
                    subscriber.process()
                except Exception as e:
                    Customers.error_handler.add_error_v(
                        error=f'Error processing subscriber {subscriber.email}: {e}',
                        origin='NewsletterSubscribers.sync',
                    )


if __name__ == '__main__':
    subs = NewsletterSubscribers()
    subs.sync()
