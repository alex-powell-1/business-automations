from datetime import datetime
import csv

import requests

from setup import creds
from setup import date_presets
from setup.create_log import create_customer_log
from database import Database as db
from setup.error_handler import ScheduledTasksErrorHandler as error_handler
from database import Database

from setup.utilities import PhoneNumber


class Customer:
    def __init__(self, number):
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
        PROF_COD_2, PROF_COD_3, {creds.sms_subscribe_status}, PROF_ALPHA_1
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

    def add_to_mailerlite(self):
        url = 'https://connect.mailerlite.com/api/subscribers/'

        headers = {
            'Authorization': f'Bearer {creds.mailerlite_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        if self.category == 'WHOLESALE':
            payload = {
                'email': self.email_1,
                'groups': [creds.wholesale_mailing_list],
                'fields': {
                    'name': self.first_name,
                    'last_name': self.last_name,
                    'company': self.name,
                    'phone': self.phone_1,
                },
            }
        else:
            payload = {
                'email': self.email_1,
                'groups': [creds.retail_all_mailing_list],
                'fields': {'name': self.first_name, 'last_name': self.last_name, 'phone': self.phone_1},
            }

        response = requests.post(url, headers=headers, json=payload)
        print(response.json())

    def unsubscribe_from_sms(self):
        query = f"""
        UPDATE AR_CUST
        SET {creds.sms_subscribe_status} = 'N', LST_MAINT_DT = GETDATE()
        WHERE CUST_NO = '{self.number}'
        """
        db.query(query)
        create_customer_log(
            customer_number=self.number,
            first_name=self.first_name,
            last_name=self.last_name,
            name=self.name,
            phone_1=self.phone_1,
            status_1_col_name='unsubscribed',
            status_1_data=f'Unsubscribed on {date_presets.today:%x}',
            log_location=creds.unsubscribed_sms,
        )

    def subscribe_to_sms(self):
        query = f"""
        UPDATE AR_CUST
        SET {creds.sms_subscribe_status} = 'Y', LST_MAINT_DT = GETDATE()
        WHERE CUST_NO = '{self.number}'
        """
        db.query(query)

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
            error_handler.logger.success(
                f'{self.name}({self.number}) pricing tier updated from {current_tier} to {target_tier}'
            )
        else:
            error_handler.error_handler.add_error_v(error=response['message'], origin='set_pricing_tier')


def export_retail_customer_csv():
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, PROF_COD_2, LOY_PTS_BAL 
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        error_handler.logger.info('Getting retail customer data from SQL')
        response = db.query(retail_query)
    except Exception as err:
        error_handler.error_handler.add_error_v(error=err, origin='Retail Customer SQL Query')
    else:
        if response is None:
            error_handler.logger.info('No Retail Data')
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

            open(creds.retail_customer_backup, 'w', encoding='utf-8')
            export_file = open(creds.retail_customer_backup, 'a')
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
        error_handler.logger.info('Retail Export Complete')


def export_wholesale_customer_csv():
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        error_handler.logger.info('Getting wholesale customer data from SQL')
        response = db.query(retail_query)
    except Exception as err:
        error_handler.error_handler.add_error_v(error=err, origin='Wholesale Customer SQL Query')
    else:
        if response is None:
            error_handler.logger.info('No Wholesale Data')
        else:
            header_list = ['Customer Number', 'First Name', 'Last Name', 'Email Address', 'Phone Number']

            open(creds.wholesale_customer_backup, 'w')
            export_file = open(creds.wholesale_customer_backup, 'a', encoding='utf-8')
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
        error_handler.logger.info('Wholesale Export Complete')


def export_customers_to_csv():
    error_handler.logger.info(f'Customer Export: Starting at {datetime.now():%H:%M:%S}')
    export_retail_customer_csv()
    export_wholesale_customer_csv()
    error_handler.logger.info(f'Customer Export: Finished at {datetime.now():%H:%M:%S}')


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


def get_customer_number_by_phone(phone):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE PHONE_1 = '{phone}'
    """
    response = db.query(query)
    if response is not None:
        return response[0][0]


def get_customer_number_by_email(email):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE EMAIL_ADRS_1 = '{email}'
    """
    response = db.query(query)
    if response is not None:
        return response[0][0]


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


def add_all_customers_to_mailerlite(category):
    customer_list = get_customers_by_category(category)
    for x in customer_list:
        customer = Customer(x)
        customer.add_to_mailerlite()


def lookup_customer_by_email(email_address):
    if email_address is None:
        return
    email_address = email_address.replace("'", "''")
    query = f"""
    SELECT TOP 1 CUST_NO
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 = '{email_address}' or EMAIL_ADRS_2 = '{email_address}'
    """
    response = db.query(query)
    if response is not None:
        return response[0][0]


def lookup_customer_by_phone(phone_number):
    if phone_number is None:
        return
    phone_number = PhoneNumber(phone_number).to_cp()
    query = f"""
    SELECT TOP 1 CUST_NO
    FROM AR_CUST
    WHERE PHONE_1 = '{phone_number}' or MBL_PHONE_1 = '{phone_number}'
    """
    response = db.query(query)
    if response is not None:
        return response[0][0]


def lookup_customer(email_address=None, phone_number=None):
    return lookup_customer_by_email(email_address) or lookup_customer_by_phone(phone_number)


def is_customer(email_address, phone_number):
    """Checks to see if an email or phone number belongs to a current customer"""
    return lookup_customer_by_email(email_address) is not None or lookup_customer_by_phone(phone_number) is not None


def add_new_customer(first_name, last_name, phone_number, email_address, street_address, city, state, zip_code):
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

    if phone_number is not None:
        phone_number = PhoneNumber(phone_number).to_cp()

    if not is_customer(email_address=email_address, phone_number=phone_number):
        url = f'{creds.cp_api_server}/CUSTOMER/'
        headers = {
            'Authorization': f'Basic {creds.cp_api_user}',
            'APIKey': creds.cp_api_key,
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
            print(f'Customer Added: {response.json()}')
        else:
            print(f'Error: {response.status_code} - {response.text}')

        cust_id = response.json()['CUST_NO']
        Database.Counterpoint.Customer.update_timestamps(customer_no=cust_id)
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

    return response


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


def set_contact_1():
    """Takes first name and last name and updates contact 1 field in counterpoint"""
    error_handler.logger.info(f'Set Contact 1: Starting at {datetime.now():%H:%M:%S}')
    target_customers = get_customers_with_no_contact_1()
    if target_customers is None:
        error_handler.logger.info('No customer_tools to set at this time.')
    else:
        error_handler.logger.info(f'{len(target_customers)} Customers to Update')
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
                        error_handler.logger.info(
                            f"Customer {x}: " f"Contact 1 updated to: {full_name.replace("''", "'")}"
                        )
                    else:
                        error_handler.error_handler.add_error_v(error=response['message'], origin='set_contact_1')

    error_handler.logger.info(f'Set Contact 1: Finished at {datetime.now():%H:%M:%S}')


def get_duplicate_customers(email=False, phone=False):
    if not email and not phone:
        return 'Please select email or phone'
    if email:
        query = """
        SELECT *
        FROM AR_CUST 
        WHERE EMAIL_ADRS_1 in (SELECT EMAIL_ADRS_1
                                FROM AR_CUST
                                GROUP BY EMAIL_ADRS_1
                                HAVING COUNT(EMAIL_ADRS_1) > 1)
        ORDER BY EMAIL_ADRS_1
        """
        response = db.query(query, mapped=True)
        if response is not None:
            return response
        else:
            return 'No Duplicates Found'
    if phone:
        query = """
        SELECT *
        FROM AR_CUST 
        WHERE PHONE_1 in (SELECT PHONE_1
                            FROM AR_CUST
                            GROUP BY PHONE_1
                            HAVING COUNT(PHONE_1) > 1)
        ORDER BY PHONE_1
        """
        response = db.query(query, mapped=True)
        if response is not None:
            return response
        else:
            return 'No Duplicates Found'


class CustomerMergeCandidate:
    def __init__(self, cust_no):
        self.customer = None
        self.shipping_addresses = []

    def get(self):
        query = f"""
        SELECT * FROM AR_CUST WHERE CUST_NO = '{self.cust_no}'
        """
        response = db.query(query)
        if response is not None:
            if response['code'] == 200:
                self.customer = self.Customer(response['data'][0])
                self.shipping_addresses = self.get_shipping_addresses()

    def get_shipping_addresses(self):
        query = f"""
        SELECT * FROM AR_SHIP_ADRS
        WHERE CUST_NO = '{self.cust_no}'
        """
        response = db.query(query, mapped=True)
        if response is not None:
            for x in response['data']:
                self.shipping_addresses.append(CustomerMergeCandidate.ShippingAddress(x))

    class Customer:
        def __init__(self, cust):
            self.CUST_NO = cust['CUST_NO']
            self.NAM = cust['NAM']
            self.NAM_UPR = cust['']
            self.FST_NAM = cust['']
            self.FST_NAM_UPR = cust['']
            self.LST_NAM = cust['']
            self.LST_NAM_UPR = cust['']
            self.SALUTATION = cust['']
            self.CUST_TYP = cust['']
            self.ADRS_1 = cust['']
            self.ADRS_2 = cust['']
            self.ADRS_3 = cust['']
            self.CITY = cust['']
            self.STATE = cust['']
            self.ZIP_COD = cust['']
            self.CNTRY = cust['']
            self.PHONE_1 = cust['']
            self.PHONE_2 = cust['']
            self.FAX_1 = cust['']
            self.FAX_2 = cust['']
            self.CONTCT_1 = cust['']
            self.CONTCT_2 = cust['']
            self.EMAIL_ADRS_1 = cust['']
            self.EMAIL_ADRS_2 = cust['']
            self.URL_1 = cust['']
            self.URL_2 = cust['']
            self.PROMPT_NAM_ADRS = cust['']
            self.SLS_REP = cust['']
            self.CATEG_COD = cust['']
            self.SHIP_VIA_COD = cust['']
            self.SHIP_ZONE_COD = cust['']
            self.STR_ID = cust['']
            self.STMNT_COD = cust['']
            self.TAX_COD = cust['']
            self.TERMS_COD = cust['']
            self.COMMNT = cust['']
            self.TAX_EXEMPT_NO = cust['']
            self.TAX_EXEMPT_DAT = cust['']
            self.ALLOW_AR_CHRG = cust['']
            self.ALLOW_TKTS = cust['']
            self.NO_CR_LIM = cust['']
            self.CR_LIM = cust['']
            self.CR_RATE = cust['']
            self.NO_MAX_CHK_AMT = cust['']
            self.MAX_CHK_AMT = cust['']
            self.UNPSTD_BAL = cust['']
            self.BAL_METH = cust['']
            self.AR_ACCT_NO = cust['']
            self.BAL = cust['']
            self.ORD_BAL = cust['']
            self.NO_OF_ORDS = cust['']
            self.USE_ORD_SHIP_TO = cust['']
            self.ALLOW_ORDS = cust['']
            self.LST_AGE_DAT = cust['']
            self.LST_AGE_BAL = cust['']
            self.LST_AGE_BAL_1 = cust['']
            self.LST_AGE_BAL_2 = cust['']
            self.LST_AGE_BAL_3 = cust['']
            self.LST_AGE_BAL_4 = cust['']
            self.LST_AGE_BAL_5 = cust['']
            self.LST_AGE_BAL_2_5 = cust['']
            self.LST_AGE_BAL_3_5 = cust['']
            self.LST_AGE_BAL_4_5 = cust['']
            self.LST_AGE_BAL_OPN = cust['']
            self.LST_AGE_FUTR_DOCS = cust['']
            self.LST_AGE_METH = cust['']
            self.LST_AGE_AS_OF_DAT = cust['']
            self.LST_AGE_CUTOFF_DAT = cust['']
            self.LST_AGE_MAX_PRD_1 = cust['']
            self.LST_AGE_MAX_PRD_2 = cust['']
            self.LST_AGE_MAX_PRD_3 = cust['']
            self.LST_AGE_MAX_PRD_4 = cust['']
            self.LST_AGE_NO_OF_PRDS = cust['']
            self.LST_AGE_EVENT_NO = cust['']
            self.LST_AGE_NO_CUTOFF = cust['']
            self.LST_AGE_PAST_CUTOFF = cust['']
            self.LST_AGE_NON_STD = cust['']
            self.LST_STMNT_DAT = cust['']
            self.LST_STMNT_BAL = cust['']
            self.LST_STMNT_BAL_1 = cust['']
            self.LST_STMNT_BAL_2 = cust['']
            self.LST_STMNT_BAL_3 = cust['']
            self.LST_STMNT_BAL_4 = cust['']
            self.LST_STMNT_BAL_5 = cust['']
            self.LST_STMNT_BAL_2_5 = cust['']
            self.LST_STMNT_BAL_3_5 = cust['']
            self.LST_STMNT_BAL_4_5 = cust['']
            self.LST_STMNT_BAL_OPN = cust['']
            self.LST_STMNT_METH = cust['']
            self.LST_STMNT_BEG_DAT = cust['']
            self.LST_STMNT_END_DAT = cust['']
            self.LST_STMNT_MAX_PRD_1 = cust['']
            self.LST_STMNT_MAX_PRD_2 = cust['']
            self.LST_STMNT_MAX_PRD_3 = cust['']
            self.LST_STMNT_MAX_PRD_4 = cust['']
            self.LST_STMNT_NO_OF_PRDS = cust['']
            self.LST_STMNT_PAST_CTOFF = cust['']
            self.FST_SAL_DAT = cust['']
            self.LST_SAL_DAT = cust['']
            self.LST_SAL_AMT = cust['']
            self.LST_PMT_DAT = cust['']
            self.LST_PMT_AMT = cust['']
            self.PROF_ALPHA_1 = cust['']
            self.PROF_ALPHA_2 = cust['']
            self.PROF_ALPHA_3 = cust['']
            self.PROF_ALPHA_4 = cust['']
            self.PROF_ALPHA_5 = cust['']
            self.PROF_COD_1 = cust['']
            self.PROF_COD_2 = cust['']
            self.PROF_COD_3 = cust['']
            self.PROF_COD_4 = cust['']
            self.PROF_COD_5 = cust['']
            self.PROF_DAT_1 = cust['']
            self.PROF_DAT_2 = cust['']
            self.PROF_DAT_3 = cust['']
            self.PROF_DAT_4 = cust['']
            self.PROF_DAT_5 = cust['']
            self.PROF_NO_1 = cust['']
            self.PROF_NO_2 = cust['']
            self.PROF_NO_3 = cust['']
            self.PROF_NO_4 = cust['']
            self.PROF_NO_5 = cust['']
            self.LST_MAINT_DT = cust['']
            self.LST_MAINT_USR_ID = cust['']
            self.LST_LCK_DT = cust['']
            self.ROW_TS = cust['']
            self.WRK_STMNT_ACTIV = cust['']
            self.LWY_BAL = cust['']
            self.NO_OF_LWYS = cust['']
            self.USE_LWY_SHIP_TO = cust['']
            self.ALLOW_LWYS = cust['']
            self.IS_ECOMM_CUST = cust['']
            self.ECOMM_CUST_NO = cust['']
            self.ECOMM_AFFIL_COD = cust['']
            self.DISC_PCT = cust['']
            self.ECOMM_INIT_PWD = cust['']
            self.ECOMM_NXT_PUB_UPDT = cust['']
            self.ECOMM_NXT_PUB_FULL = cust['']
            self.ECOMM_LST_PUB_DT = cust['']
            self.ECOMM_LST_PUB_TYP = cust['']
            self.ECOMM_LST_IMP_DT = cust['']
            self.ECOMM_CREATED_CUST = cust['']
            self.ECOMM_LST_ORD_NO = cust['']
            self.ECOMM_LST_ORD_DT = cust['']
            self.ECOMM_LST_IMP_TYP = cust['']
            self.ECOMM_LST_IMP_EVENT_NO = cust['']
            self.PROMPT_FOR_CUSTOM_FLDS = cust['']
            self.LOY_PGM_COD = cust['']
            self.LOY_PTS_BAL = cust['']
            self.TOT_LOY_PTS_EARND = cust['']
            self.TOT_LOY_PTS_RDM = cust['']
            self.TOT_LOY_PTS_ADJ = cust['']
            self.LST_LOY_EARN_TKT_DAT = cust['']
            self.LST_LOY_EARN_TKT_TIM = cust['']
            self.LST_LOY_PTS_EARN = cust['']
            self.LST_LOY_EARN_TKT_NO = cust['']
            self.LST_LOY_RDM_TKT_DAT = cust['']
            self.LST_LOY_RDM_TKT_TIM = cust['']
            self.LST_LOY_PTS_RDM = cust['']
            self.LST_LOY_RDM_TKT_NO = cust['']
            self.LST_LOY_ADJ_DAT = cust['']
            self.LST_LOY_PTS_ADJ = cust['']
            self.LST_LOY_ADJ_DOC_NO = cust['']
            self.LOY_CARD_NO = cust['']
            self.FCH_COD = cust['']
            self.LST_FCH_DAT = cust['']
            self.LST_FCH_AMT = cust['']
            self.LST_FCH_PAST_DUE_AMT = cust['']
            self.LST_FCH_DOC_NO = cust['']
            self.REQ_PO_NO = cust['']
            self.RS_UTC_DT = cust['']
            self.CUST_NAM_TYP = cust['']
            self.CUST_FST_LST_NAM = cust['']
            self.LST_LOY_EARN_TKT_DT = cust['']
            self.LST_LOY_RDM_TKT_DT = cust['']
            self.PS_HDR_CUST_FLD_FRM_ID = cust['']
            self.EMAIL_STATEMENT = cust['']
            self.RS_STAT = cust['']
            self.INCLUDE_IN_MARKETING_MAILOUTS = cust['']
            self.MARKETING_MAILOUT_OPT_IN_DAT = cust['']
            self.RPT_EMAIL = cust['']
            self.MBL_PHONE_1 = cust['']
            self.MBL_PHONE_2 = cust['']
            self.SUBSCRIBED_SMS = cust['']
            self.SUBSCRIBED_EMAIL = cust['']

    class ShippingAddress:
        def __init__(self, cust):
            self.CUST_NO = cust['CUST_NO']
            self.SHIP_ADRS_ID = cust['SHIP_ADRS_ID']
            self.NAM = cust['NAM']
            self.NAM_UPR = cust['NAM_UPR']
            self.FST_NAM = cust['FST_NAM']
            self.FST_NAM_UPR = cust['FST_NAM_UPR']
            self.LST_NAM = cust['LST_NAM']
            self.LST_NAM_UPR = cust['LST_NAM_UPR']
            self.ADRS_1 = cust['ADRS_1']
            self.ADRS_2 = cust['ADRS_2']
            self.ADRS_3 = cust['ADRS_3']
            self.CITY = cust['CITY']
            self.STATE = cust['STATE']
            self.ZIP_COD = cust['ZIP_COD']
            self.CNTRY = cust['CNTRY']
            self.PHONE_1 = cust['PHONE_1']
            self.PHONE_2 = cust['PHONE_2']
            self.FAX_1 = cust['FAX_1']
            self.FAX_2 = cust['FAX_2']
            self.CONTCT_1 = cust['CONTCT_1']
            self.CONTCT_2 = cust['CONTCT_2']
            self.EMAIL_ADRS_1 = cust['EMAIL_ADRS_1']
            self.EMAIL_ADRS_2 = cust['EMAIL_ADRS_2']
            self.SHIP_NAM_TYP = cust['SHIP_NAM_TYP']
            self.SHIP_FST_LST_NAM = cust['SHIP_FST_LST_NAM']

        def __str__(self) -> str:
            return f'{self.NAM} - {self.ADRS_1} {self.CITY}, {self.STATE} {self.ZIP_COD}'


if __name__ == '__main__':
    cust = CustomerMergeCandidate('OL-100168')
    cust.get_shipping_addresses()
    for x in cust.shipping_addresses:
        # Get all properties for the object - keys only
        keys = x.__dict__.keys()
        for x in keys:
            print(x)

    response = Database.query('SELECT TOP 1 * FROM AR_CUST', mapped=True)
    print(response)
