import json
from datetime import datetime
import csv

import requests

from setup import creds
from setup import date_presets
from setup.create_log import create_customer_log
from setup.query_engine import QueryEngine

db = QueryEngine()


class Customer:
    def __init__(self, number):
        self.number = number
        self.first_name = ""
        self.last_name = ""
        self.name = ""
        self.phone_1 = ""
        self.mbl_phone_1 = ""
        self.phone_2 = 0
        self.mbl_phone_2 = ""
        self.email_1 = ""
        self.email_2 = ""
        self.address = ""
        self.mbl_phone_1 = ""
        self.customer_type = ""
        self.city = ""
        self.state = ""
        self.zip = ""
        self.category = ""
        self.price_tier = ""
        self.rewards_points_balance = ""
        self.loyalty_program = ""
        self.birth_month = ""
        self.spouse_birth_month = ""
        self.sms_subscribe = ""
        self.pricing_tier = ""
        self.set_customer_details()

    def set_customer_details(self):
        query = f"""
        SELECT FST_NAM, LST_NAM, NAM, PHONE_1, MBL_PHONE_1,
        PHONE_2, MBL_PHONE_2, EMAIL_ADRS_1, EMAIL_ADRS_2, ADRS_1,
        CITY, STATE, ZIP_COD, CUST_TYP, CATEG_COD, PROF_COD_1, ISNULL(LOY_PTS_BAL, 0), LOY_PGM_COD,
        PROF_COD_2, PROF_COD_3, INCLUDE_IN_MARKETING_MAILOUTS, PROF_ALPHA_1
        FROM AR_CUST
        WHERE CUST_NO = '{self.number}'
        """
        response = db.query_db(query)
        if response is not None:
            self.first_name = response[0][0] if response[0][0] is not None else ""
            self.last_name = response[0][1] if response[0][1] is not None else ""
            self.name = response[0][2] if response[0][2] is not None else ""
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
        url = "https://connect.mailerlite.com/api/subscribers/"

        headers = {
            "Authorization": f"Bearer {creds.mailerlite_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        if self.category == 'WHOLESALE':
            payload = {
                "email": self.email_1,
                "groups": [creds.wholesale_mailing_list],
                "fields": {
                    "name": self.first_name,
                    "last_name": self.last_name,
                    "company": self.name,
                    "phone": self.phone_1
                }
            }
        else:
            payload = {
                "email": self.email_1,
                "groups": [creds.retail_all_mailing_list],
                "fields": {
                    "name": self.first_name,
                    "last_name": self.last_name,
                    "phone": self.phone_1
                }
            }

        response = requests.post(url, headers=headers, json=payload)
        print(response.json())

    def unsubscribe_from_sms(self):
        query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'N', LST_MAINT_DT = GETDATE()
        WHERE CUST_NO = '{self.number}'
        """
        db.query_db(query, commit=True)
        create_customer_log(customer_number=self.number,
                            first_name=self.first_name,
                            last_name=self.last_name,
                            name=self.name,
                            phone_1=self.phone_1,
                            status_1_col_name="unsubscribed",
                            status_1_data=f"Unsubscribed on {date_presets.today:%x}",
                            log_location=creds.unsubscribed_sms)

    def subscribe_to_sms(self):
        query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'Y', LST_MAINT_DT = GETDATE()
        WHERE CUST_NO = '{self.number}'
        """
        db.query_db(query, commit=True)

    def get_total_spent(self, start_date, stop_date):
        pass

    def get_pricing_tier(self):
        # Get Current Pricing Tier Level
        query = f"""
                SELECT PROF_ALPHA_1
                FROM AR_CUST
                WHERE CUST_NO = '{self.number}'
                """
        response = db.query_db(query)
        if response is not None:
            tier = response[0][0]
        else:
            tier = None
        return tier

    def set_pricing_tier(self, target_tier, log_file):
        current_tier = self.get_pricing_tier()
        # Set New Pricing Level
        query = f"""
                UPDATE AR_CUST
                SET PROF_ALPHA_1 = {int(target_tier)}
                WHERE CUST_NO = '{self.number}'
                """
        db.query_db(query, commit=True)
        print(f"{self.name}({self.number}) pricing tier updated from {current_tier} to {target_tier}", file=log_file)

# --------------------------------------------


def export_retail_customer_csv(log_file):
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, PROF_COD_2, LOY_PTS_BAL 
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        print("Getting retail customer data from SQL", file=log_file)
        response = db.query_db(retail_query)
    except Exception as err:
        print("Error: Retail Customer SQL Query", file=log_file)
        print(err, file=log_file)
        print("-----------------------\n", file=log_file)
    else:
        if response is None:
            print("No Retail Data", file=log_file)
        else:
            header_list = ['Customer Number', 'First Name', 'Last Name', 'Email Address',
                           'Phone - home', 'Birth Month', 'Point Balance']

            open(creds.retail_customer_backup, 'w')
            export_file = open(creds.retail_customer_backup, 'a')
            w = csv.writer(export_file)

            w.writerow(header_list)

            for x in response:
                customer_number = x[0]
                first_name = x[1]
                last_name = x[2]
                email_address = x[3]
                phone = x[4]
                # Change nulls to empty string
                birth_month = int(x[5]) if x[5] is not None else ""
                # Change nulls to 0. Change Negative Numbers to 0
                point_balance = int(x[6]) if x[6] is not None or int(x[6]) >= 0 else 0

                w.writerow([customer_number, first_name, last_name, email_address, phone, birth_month, point_balance])

            export_file.close()
    finally:
        print("Retail Export Complete")


def export_wholesale_customer_csv(log_file):
    retail_query = """
    SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1
    FROM AR_CUST 
    WHERE EMAIL_ADRS_1 IS NOT NULL AND CATEG_COD = 'RETAIL'
    """

    try:
        print("Getting wholesale customer data from SQL", file=log_file)
        response = db.query_db(retail_query)
    except Exception as err:
        print("Error: Retail Customer SQL Query", file=log_file)
        print(err, file=log_file)
        print("-----------------------\n", file=log_file)
    else:
        if response is None:
            print("No Wholesale Data", file=log_file)
        else:
            header_list = ['Customer Number', 'First Name', 'Last Name', 'Email Address',
                           'Phone Number']

            open(creds.wholesale_customer_backup, 'w')
            export_file = open(creds.wholesale_customer_backup, 'a')
            w = csv.writer(export_file)

            w.writerow(header_list)

            for x in response:
                customer_number = x[0]
                first_name = x[1]
                last_name = x[2]
                email_address = x[3]
                phone = x[4]

                w.writerow([customer_number, first_name, last_name, email_address, phone])

            export_file.close()
    finally:
        print("Wholesale Export Complete")


def export_customers_to_csv(log_file):
    print(f"Customer Export: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    export_retail_customer_csv(log_file)
    export_wholesale_customer_csv(log_file)
    print(f"Customer Export: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def is_current_customer(customer_number):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CUST_NO = '{customer_number}'
    """
    response = db.query_db(query)
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
    response = db.query_db(query)
    if response is not None:
        return response[0][0]


def get_customer_number_by_email(email):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE EMAIL_ADRS_1 = '{email}'
    """
    response = db.query_db(query)
    if response is not None:
        return response[0][0]


def get_customers_by_category(category):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CATEG_COD = '{category}' AND EMAIL_ADRS_1 IS NOT NULL
    """
    response = db.query_db(query)
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
    query = f"""
    SELECT TOP 1 CUST_NO
    FROM AR_CUST
    WHERE EMAIL_ADRS_1 = '{email_address}' or EMAIL_ADRS_2 = '{email_address}'
    """
    response = db.query_db(query)
    if response is not None:
        return response[0][0]


def lookup_customer_by_phone(phone_number):
    query = f"""
    SELECT TOP 1 CUST_NO
    FROM AR_CUST
    WHERE PHONE_1 = '{phone_number}' or MBL_PHONE_1 = '{phone_number}'
    """
    response = db.query_db(query)
    if response is not None:
        return response[0][0]


def is_customer(email_address, phone_number):
    """Checks to see if an email or phone number belongs to a current customer"""
    return (lookup_customer_by_email(email_address) is not None or
            lookup_customer_by_phone(phone_number) is not None)


def add_new_customer(first_name, last_name, phone_number,
                     email_address, street_address, city, state, zip_code):
    if not is_customer(email_address=email_address, phone_number=phone_number):
        url = f'{creds.cp_api_server}/CUSTOMER/'
        headers = {
            'Authorization': f'Basic {creds.cp_api_user}',
            'APIKey': creds.cp_api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        payload = {
            "Workgroup": "1",
            "AR_CUST": {
                "FST_NAM": first_name,
                "LST_NAM": last_name,
                "STR_ID": "1",
                'EMAIL_ADRS_1': email_address,
                'PHONE_1': phone_number,
                'ADRS_1': street_address,
                'CITY': city,
                'STATE': state,
                'ZIP_COD': zip_code,
            }
        }

        response = requests.post(url, headers=headers, verify=False, json=payload)
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        print(pretty)
        return response.json()['CUST_NO']
    else:
        return "Already a customer"


def get_customers_with_negative_loyalty():
    query = """
    SELECT CUST_NO 
    FROM AR_CUST
    WHERE LOY_PTS_BAL < 0
    """
    response = db.query_db(query)

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
    response = db.query_db(query)

    if response is not None:
        target_customers = []
        for customer in response:
            target_customers.append(customer[0])
        return target_customers


def set_negative_loyalty_points_to_zero(log_file):
    print(f"Set Negative Loyalty to 0: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    target_customers = get_customers_with_negative_loyalty()
    if target_customers is not None:
        print(f"{len(target_customers)} Customers to Update", file=log_file)

        for x in target_customers:
            query = f"""
            UPDATE AR_CUST
            SET LOY_PTS_BAL = 0
            WHERE CUST_NO = '{x}'
            """
            try:
                db.query_db(query, commit=True)
            except Exception as err:
                print(f"Error: {x} - {err}", file=log_file)
            else:
                print(f"Customer {x} Updated to Loyalty Points: 0")
    else:
        print(f"No Customers to Update", file=log_file)

    print(f"Set Contact 1: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)


def set_contact_1(log_file):
    """Takes first name and last name and updates contact 1 field in counterpoint"""
    print(f"Set Contact 1: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    target_customers = get_customers_with_no_contact_1()
    if target_customers is None:
        print("No customer_tools to set at this time.", file=log_file)
    else:
        print(f"{len(target_customers)} Customers to Update", file=log_file)
        for x in target_customers:
            query = f"""
            SELECT FST_NAM, LST_NAM
            FROM AR_CUST
            WHERE CUST_NO = '{x}'
            """
            response = db.query_db(query)

            if response is not None:
                for y in response:
                    first_name = y[0]
                    last_name = y[1]
                    full_name = f"{str(first_name).title()} {str(last_name).title()}"

                    # In SQL, we must replace single quotes with two single quotes Kim O'Hare --> Kim O''Hare
                    full_name = full_name.replace("'", "''")

                    # Update Customer with full name as new contact 1.
                    query = f"""
                    UPDATE AR_CUST
                    SET CONTCT_1 = '{full_name}'
                    WHERE CUST_NO = '{x}'
                    """
                    try:
                        db.query_db(query, commit=True)
                    except Exception as err:
                        print(f"Error: {x} - {err}", file=log_file)
                    else:
                        print(f"Customer {x}: "
                              f"Contact 1 updated to: {full_name.replace("''", "'")}", file=log_file)

    print(f"Set Contact 1: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)