import csv
import requests
from datetime import datetime
from setup import creds
from database import Database as db
from setup.error_handler import ScheduledTasksErrorHandler, ProcessInErrorHandler
from integration.models.shopify_orders import ShopifyOrder
from setup.utilities import PhoneNumber, states


class Customer:
    def __init__(self, number):
        self.logger = ScheduledTasksErrorHandler.logger
        self.error_handler = ScheduledTasksErrorHandler.error_handler
        self.number = number
        self.first_name = ''
        self.last_name = ''
        self.name = ''
        self.phone_1 = ''
        self.mbl_phone_1 = ''
        self.phone_2 = 0
        self.mbl_phone_2 = ''
        self.email_1 = ''
        self.email_2 = ''
        self.address = ''
        self.mbl_phone_1 = ''
        self.customer_type = ''
        self.city = ''
        self.state = ''
        self.zip = ''
        self.category = ''
        self.price_tier = ''
        self.rewards_points_balance = ''
        self.loyalty_program = ''
        self.birth_month = ''
        self.spouse_birth_month = ''
        self.sms_subscribe = ''
        self.pricing_tier = ''
        self.set_customer_details()

    def set_customer_details(self):
        query = f"""
        SELECT FST_NAM, LST_NAM, NAM, PHONE_1, MBL_PHONE_1,
        PHONE_2, MBL_PHONE_2, EMAIL_ADRS_1, EMAIL_ADRS_2, ADRS_1,
        CITY, STATE, ZIP_COD, CUST_TYP, CATEG_COD, PROF_COD_1, ISNULL(LOY_PTS_BAL, 0), LOY_PGM_COD,
        PROF_COD_2, PROF_COD_3, {creds.Table.CP.Customers.Column.sms_1_is_subscribed}, PROF_ALPHA_1
        FROM AR_CUST
        WHERE CUST_NO = '{self.number}'
        """
        response = db.query(query)
        if response is not None:
            self.first_name = response[0][0] if response[0][0] is not None else ''
            self.last_name = response[0][1] if response[0][1] is not None else ''
            self.name = response[0][2] if response[0][2] is not None else ''
            self.phone_1 = response[0][3]
            self.mbl_phone_1 = response[0][4]
            self.phone_2 = response[0][5]
            self.mbl_phone_2 = response[0][6]
            self.email_1 = response[0][7]
            self.email_2 = response[0][8]
            self.address = response[0][9]
            self.city = response[0][10]
            self.state = response[0][11]
            self.zip = response[0][12]
            self.customer_type = response[0][13]
            self.category = response[0][14]
            self.price_tier = response[0][15]
            self.rewards_points_balance = int(response[0][16])
            self.loyalty_program = response[0][17]
            self.birth_month = response[0][18]
            self.spouse_birth_month = response[0][19]
            self.sms_subscribe = response[0][20]
            self.pricing_tier = int(response[0][21]) if response[0][21] is not None else None

    def get_total_spent(self, start_date, stop_date):
        pass

    def get_pricing_tier(self):
        # Get Current Pricing Tier Level
        query = f"""
                SELECT PROF_ALPHA_1
                FROM AR_CUST
                WHERE CUST_NO = '{self.number}'
                """
        response = db.query(query)
        if response is not None:
            tier = response[0][0]
        else:
            tier = None
        return tier

    def set_pricing_tier(self, target_tier):
        current_tier = self.get_pricing_tier()
        # Set New Pricing Level
        query = f"""
                UPDATE AR_CUST
                SET PROF_ALPHA_1 = {int(target_tier)}
                WHERE CUST_NO = '{self.number}'
                """
        response = db.query(query)
        if response['code'] == 200:
            self.logger.success(
                f'{self.name}({self.number}) pricing tier updated from {current_tier} to {target_tier}'
            )
        else:
            self.error_handler.add_error_v(error=response['message'], origin='set_pricing_tier')


def export_retail_customer_csv(eh=ScheduledTasksErrorHandler):
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, PROF_COD_2, LOY_PTS_BAL 
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        eh.logger.info('Getting retail customer data from SQL')
        response = db.query(retail_query)
    except Exception as err:
        eh.error_handler.add_error_v(error=err, origin='Retail Customer SQL Query')
    else:
        if response is None:
            eh.logger.info('No Retail Data')
        else:
            header_list = [
                'Customer Number',
                'First Name',
                'Last Name',
                'Email Address',
                'Phone - home',
                'Birth Month',
                'Point Balance',
            ]

            open(creds.Backups.Customer.retail, 'w', encoding='utf-8')
            export_file = open(creds.Backups.Customer.retail, 'a')
            w = csv.writer(export_file)

            w.writerow(header_list)

            for x in response:
                customer_number = x[0] if x[0] is not None else ''
                first_name = x[1] if x[1] is not None else ''
                last_name = x[2] if x[2] is not None else ''
                email_address = x[3] if x[3] is not None else ''
                phone = x[4] if x[4] is not None else ''
                # Change nulls to empty string
                birth_month = int(x[5]) if x[5] is not None else ''
                # Change nulls to 0. Change Negative Numbers to 0
                point_balance = int(x[6]) if x[6] is not None or int(x[6]) >= 0 else 0

                w.writerow(
                    [customer_number, first_name, last_name, email_address, phone, birth_month, point_balance]
                )

            export_file.close()
            print('HERE')
    finally:
        eh.logger.info('Retail Export Complete')


def export_wholesale_customer_csv(eh=ScheduledTasksErrorHandler):
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        eh.logger.info('Getting wholesale customer data from SQL')
        response = db.query(retail_query)
    except Exception as err:
        eh.error_handler.add_error_v(error=err, origin='Wholesale Customer SQL Query')
    else:
        if response is None:
            eh.logger.info('No Wholesale Data')
        else:
            header_list = ['Customer Number', 'First Name', 'Last Name', 'Email Address', 'Phone Number']

            open(creds.Backups.Customer.wholesale, 'w')
            export_file = open(creds.Backups.Customer.wholesale, 'a', encoding='utf-8')
            w = csv.writer(export_file)

            w.writerow(header_list)

            for x in response:
                customer_number = x[0] if x[0] is not None else ''
                first_name = x[1] if x[1] is not None else ''
                last_name = x[2] if x[2] is not None else ''
                email_address = x[3] if x[3] is not None else ''
                phone = x[4] if x[4] is not None else ''

                w.writerow([customer_number, first_name, last_name, email_address, phone])

            export_file.close()
    finally:
        eh.logger.info('Wholesale Export Complete')


def export_customers_to_csv(eh=ScheduledTasksErrorHandler):
    eh.logger.info(f'Customer Export: Starting at {datetime.now():%H:%M:%S}')
    export_retail_customer_csv()
    export_wholesale_customer_csv()
    eh.logger.info(f'Customer Export: Finished at {datetime.now():%H:%M:%S}')


def is_current_customer(customer_number):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CUST_NO = '{customer_number}'
    """
    response = db.query(query)
    if response is not None:
        return True
    else:
        return False


def get_customers_by_category(category):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CATEG_COD = '{category}' AND EMAIL_ADRS_1 IS NOT NULL
    """
    response = db.query(query)
    if response is not None:
        customer_list = []
        for x in response:
            cust_no = x[0]
            customer_list.append(cust_no)
        return customer_list


def add_new_customer(
    first_name,
    last_name,
    phone_number,
    email_address,
    street_address,
    city,
    state,
    zip_code,
    eh=ProcessInErrorHandler,
) -> str:
    """Add a new customer to Counterpoint and returns customer number"""

    if phone_number is not None:
        phone_number = PhoneNumber(phone_number).to_cp()

    if not db.CP.Customer.is_customer(email_address=email_address, phone_number=phone_number):
        url = f'{creds.Counterpoint.API.server}/CUSTOMER/'
        headers = {
            'Authorization': f'Basic {creds.Counterpoint.API.user}',
            'APIKey': creds.Counterpoint.API.key,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        try:
            state = states[state]
        except KeyError:
            try:
                state = state[0:10]
            except:
                state = None

        if first_name is None:
            first_name = 'WEB'

        if last_name is None:
            last_name = 'CUSTOMER'

        payload = {
            'Workgroup': '1',
            'AR_CUST': {'FST_NAM': first_name, 'LST_NAM': last_name, 'STR_ID': '1', 'EMAIL_ADRS_1': email_address},
        }

        if phone_number is not None:
            payload['AR_CUST']['PHONE_1'] = phone_number

        if street_address is not None:
            payload['AR_CUST']['ADRS_1'] = street_address

        if city is not None:
            payload['AR_CUST']['CITY'] = city

        if state is not None:
            payload['AR_CUST']['STATE'] = state

        if zip_code is not None:
            payload['AR_CUST']['ZIP_COD'] = zip_code

        response = requests.post(url, headers=headers, verify=False, json=payload)

        if response.status_code in [200, 201]:
            eh.logger.success(f'Customer Added: {response.json()}')
        else:
            eh.error_handler.add_error_v(f'Error: {response.status_code} - {response.text}')

        cust_id = response.json()['CUST_NO']
        eh.logger.success(f'Customer {cust_id} created.')
        db.CP.Customer.update_timestamps(customer_no=cust_id)

        return cust_id
    else:
        return 'Already a customer'


def update_customer(
    cust_no: str | int,
    first_name: str,
    last_name: str,
    phone_number: str,
    email_address: str,
    street_address: str,
    city: str,
    state: str,
    zip_code: str,
    eh=ProcessInErrorHandler,
):
    if state is not None:
        try:
            state = states[state]
        except KeyError:
            try:
                state = state[0:10]
            except:
                state = None

    if phone_number is not None:
        phone_number = PhoneNumber(phone_number).to_cp()

    query = 'UPDATE AR_CUST SET LST_MAINT_DT = GETDATE()'

    if first_name is not None:
        FST_NAM = first_name.title().strip()

        query += f", FST_NAM = '{FST_NAM}'"
        query += f", FST_NAM_UPR = '{FST_NAM.upper()}'"

    if last_name is not None:
        LST_NAM = last_name.title().strip()

        query += f", LST_NAM = '{LST_NAM}'"
        query += f", LST_NAM_UPR = '{LST_NAM.upper()}'"

    if first_name is not None and last_name is not None:
        FST_NAM = first_name.title().strip()
        LST_NAM = last_name.title().strip()
        NAM = f'{FST_NAM} {LST_NAM}'

        query += f", NAM = '{NAM}'"
        query += f", NAM_UPR = '{NAM.upper()}'"
        query += f", CONTCT_1 = '{NAM}'"

    if phone_number is not None:
        query += f", PHONE_1 = '{phone_number}'"

    if email_address is not None:
        query += f", EMAIL_ADRS_1 = '{email_address}'"

    if street_address is not None:
        query += f", ADRS_1 = '{street_address}'"

    if city is not None:
        query += f", CITY = '{city}'"

    if state is not None:
        query += f", STATE = '{state}'"

    if zip_code is not None:
        query += f", ZIP_COD = '{zip_code}'"

    query += f" WHERE CUST_NO = '{cust_no}'"

    response = db.query(query)
    if response['code'] == 200:
        eh.logger.success('Customer updated in Counterpoint')
    else:
        eh.error_handler.add_error_v('Customer could not be updated in Counterpoint')
        eh.error_handler.add_error_v(response['message'])

    return response


def get_cp_cust_no(order: 'ShopifyOrder') -> str:
    """Takes a ShopifyOrder and returns the CP Customer ID"""
    email = order.email
    if not email and order.billing_address.email:
        email = order.billing_address.email

    if not email and order.shipping_address:
        email = order.shipping_address.email

    phone = None

    if order.billing_address.phone:
        phone = order.billing_address.phone

    if not phone and order.shipping_address:
        phone = order.shipping_address.phone

    return db.CP.Customer.lookup_customer(email, phone)


def update_customer_shipping(
    cust_no: str | int,
    first_name: str,
    last_name: str,
    phone_number: str,
    email_address: str,
    street_address: str,
    city: str,
    state: str,
    zip_code: str,
):
    states = {
        'Alabama': 'AL',
        'Alaska': 'AK',
        'Arizona': 'AZ',
        'Arkansas': 'AR',
        'California': 'CA',
        'Colorado': 'CO',
        'Connecticut': 'CT',
        'Delaware': 'DE',
        'Florida': 'FL',
        'Georgia': 'GA',
        'Hawaii': 'HI',
        'Idaho': 'ID',
        'Illinois': 'IL',
        'Indiana': 'IN',
        'Iowa': 'IA',
        'Kansas': 'KS',
        'Kentucky': 'KY',
        'Louisiana': 'LA',
        'Maine': 'ME',
        'Maryland': 'MD',
        'Massachusetts': 'MA',
        'Michigan': 'MI',
        'Minnesota': 'MN',
        'Mississippi': 'MS',
        'Missouri': 'MO',
        'Montana': 'MT',
        'Nebraska': 'NE',
        'Nevada': 'NV',
        'New Hampshire': 'NH',
        'New Jersey': 'NJ',
        'New Mexico': 'NM',
        'New York': 'NY',
        'North Carolina': 'NC',
        'North Dakota': 'ND',
        'Ohio': 'OH',
        'Oklahoma': 'OK',
        'Oregon': 'OR',
        'Pennsylvania': 'PA',
        'Rhode Island': 'RI',
        'South Carolina': 'SC',
        'South Dakota': 'SD',
        'Tennessee': 'TN',
        'Texas': 'TX',
        'Utah': 'UT',
        'Vermont': 'VT',
        'Virginia': 'VA',
        'Washington': 'WA',
        'West Virginia': 'WV',
        'Wisconsin': 'WI',
        'Wyoming': 'WY',
    }

    state = states[state] or state

    phone_number = PhoneNumber(phone_number).to_cp()

    FST_NAM = first_name.title().strip()
    LST_NAM = last_name.title().strip()
    NAM = f'{FST_NAM} {LST_NAM}'
    NAM_UPR = NAM.upper()
    FST_NAM_UPR = FST_NAM.upper()
    LST_NAM_UPR = LST_NAM.upper()

    query = f"""
    SELECT CUST_NO FROM AR_SHIP_ADRS WHERE CUST_NO = '{cust_no}'
    """

    response = db.query(query)

    has_user = False

    if response is not None and len(response) > 0:
        has_user = True

    if has_user:
        query = f"""
        UPDATE AR_SHIP_ADRS
        SET FST_NAM = '{FST_NAM}',
        LST_NAM = '{LST_NAM}',
        NAM = '{NAM}',
        NAM_UPR = '{NAM_UPR}',
        FST_NAM_UPR = '{FST_NAM_UPR}',
        LST_NAM_UPR = '{LST_NAM_UPR}',
        PHONE_1 = '{phone_number}',
        EMAIL_ADRS_1 = '{email_address}',
        ADRS_1 = '{street_address}',
        CITY = '{city}',
        STATE = '{state}',
        ZIP_COD = '{zip_code}',
        CONTCT_1 = '{NAM}'
        WHERE CUST_NO = '{cust_no}'
        """
    else:
        query = f"""
        INSERT INTO AR_SHIP_ADRS
        (CUST_NO, FST_NAM, LST_NAM, NAM, NAM_UPR, FST_NAM_UPR, LST_NAM_UPR, PHONE_1, EMAIL_ADRS_1, ADRS_1, CITY, STATE, ZIP_COD, CONTCT_1)
        VALUES
        ('{cust_no}', '{FST_NAM}', '{LST_NAM}', '{NAM}', '{NAM_UPR}', '{FST_NAM_UPR}', '{LST_NAM_UPR}', '{phone_number}', '{email_address}', '{street_address}', '{city}', '{state}', '{zip_code}', '{NAM}')
        """

    response = db.query(query)

    return response


def get_customers_with_negative_loyalty():
    query = """
    SELECT CUST_NO 
    FROM AR_CUST
    WHERE LOY_PTS_BAL < 0
    """
    response = db.query(query)

    if response is not None:
        target_customers = []
        for customer in response:
            target_customers.append(customer[0])
        return target_customers


def get_customers_with_no_contact_1():
    query = """
    SELECT CUST_NO 
    FROM AR_CUST
    WHERE CUST_NAM_TYP = 'p' AND
    CONTCT_1 IS NULL AND FST_NAM != 'Change' AND LST_NAM != 'Name'
    """
    response = db.query(query)

    if response is not None:
        target_customers = []
        for customer in response:
            target_customers.append(customer[0])
        return target_customers


def set_negative_loyalty_points_to_zero(log_file):
    print(f'Set Negative Loyalty to 0: Starting at {datetime.now():%H:%M:%S}', file=log_file)
    target_customers = get_customers_with_negative_loyalty()
    if target_customers is not None:
        print(f'{len(target_customers)} Customers to Update', file=log_file)

        for x in target_customers:
            query = f"""
            UPDATE AR_CUST
            SET LOY_PTS_BAL = 0
            WHERE CUST_NO = '{x}'
            """
            try:
                db.query(query)
            except Exception as err:
                print(f'Error: {x} - {err}', file=log_file)
            else:
                print(f'Customer {x} Updated to Loyalty Points: 0')
    else:
        print('No Customers to Update', file=log_file)

    print(f'Set Contact 1: Finished at {datetime.now():%H:%M:%S}', file=log_file)
    print('-----------------------', file=log_file)


def set_contact_1(eh=ScheduledTasksErrorHandler):
    """Takes first name and last name and updates contact 1 field in counterpoint"""
    eh.logger.info(f'Set Contact 1: Starting at {datetime.now():%H:%M:%S}')
    target_customers = get_customers_with_no_contact_1()
    if target_customers is None:
        eh.logger.info('No customer_tools to set at this time.')
    else:
        eh.logger.info(f'{len(target_customers)} Customers to Update')
        for x in target_customers:
            query = f"""
            SELECT FST_NAM, LST_NAM
            FROM AR_CUST
            WHERE CUST_NO = '{x}'
            """
            response = db.query(query)

            if response is not None:
                for y in response:
                    first_name = y[0]
                    last_name = y[1]
                    full_name = f'{str(first_name).title()} {str(last_name).title()}'

                    # In SQL, we must replace single quotes with two single quotes Kim O'Hare --> Kim O''Hare
                    full_name = full_name.replace("'", "''")

                    # Update Customer with full name as new contact 1.
                    query = f"""
                    UPDATE AR_CUST
                    SET CONTCT_1 = '{full_name}'
                    WHERE CUST_NO = '{x}'
                    """
                    response = db.query(query)
                    if response['code'] == 200:
                        eh.logger.info(f"Customer {x}: " f"Contact 1 updated to: {full_name.replace("''", "'")}")
                    else:
                        eh.error_handler.add_error_v(error=response['message'], origin='set_contact_1')

    eh.logger.info(f'Set Contact 1: Finished at {datetime.now():%H:%M:%S}')


def fix_first_and_last_sale_dates(dt, eh=ScheduledTasksErrorHandler):
    """Updates the first and last sale dates for customers with refunds."""
    eh.logger.info(f'Fix First and Last Sale Dates: Starting at {datetime.now():%H:%M:%S}')
    # Get a list of customers with refunds from a day ago
    customers = db.CP.ClosedOrder.get_refund_customers(dt.yesterday)
    if not customers:
        eh.logger.info(f'No customers with refunds on {dt.yesterday}.')
    else:
        for customer in customers:
            #################################
            # Fix Last Sale Date and Amount #
            #################################

            # Get the last successful order for the customer
            last_success_tkt_no = db.CP.ClosedOrder.get_last_successful_order(customer)
            if last_success_tkt_no:
                # Get the date and amount of the last successful order
                last_success_order_date = db.CP.ClosedOrder.get_business_date(tkt_no=last_success_tkt_no)
                last_success_order_amt = db.CP.ClosedOrder.get_total(tkt_no=last_success_tkt_no)
                # Update the customer's last sale date and amount
                db.CP.Customer.update_last_sale_date(
                    cust_no=customer, last_sale_date=last_success_order_date, last_sale_amt=last_success_order_amt
                )
            else:
                # If the customer has no successful orders, set the last sale date to None
                db.CP.Customer.update_last_sale_date(cust_no=customer, last_sale_date=None)

            ########################
            # Fix First Sale Date #
            ########################
            # Get the first successful order for the customer
            first_success_tkt_no = db.CP.ClosedOrder.get_first_successful_order(customer)
            if first_success_tkt_no:
                # Get the date of the first successful order
                first_success_order_date = db.CP.ClosedOrder.get_business_date(tkt_no=first_success_tkt_no)
                # Update the customer's first sale date
                db.CP.Customer.update_first_sale_date(cust_no=customer, first_sale_date=first_success_order_date)
            else:
                # If the customer has no successful orders, set the first sale date to None
                db.CP.Customer.update_first_sale_date(cust_no=customer, first_sale_date=None)

    eh.logger.info(f'Fix First and Last Sale Dates: Finished at {datetime.now():%H:%M:%S}')


if __name__ == '__main__':
    pass
