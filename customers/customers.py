from setup.query_engine import QueryEngine
from datetime import datetime
from setup import creds
from setup.create_log import create_customer_log
import requests

db = QueryEngine()


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
        self.set_customer_details()

    def set_customer_details(self):
        query = f"""
        SELECT FST_NAM, LST_NAM, NAM, PHONE_1, MBL_PHONE_1,
        PHONE_2, MBL_PHONE_2, EMAIL_ADRS_1, EMAIL_ADRS_2, ADRS_1,
        CITY, STATE, ZIP_COD, CUST_TYP, CATEG_COD, PROF_COD_1, ISNULL(LOY_PTS_BAL, 0), LOY_PGM_COD,
        PROF_COD_2, PROF_COD_3, INCLUDE_IN_MARKETING_MAILOUTS
        FROM AR_CUST
        WHERE CUST_NO = '{self.number}'
        """
        response = db.query_db(query)
        if response is not None:
            self.first_name = response[0][0]
            self.last_name = response[0][1]
            self.name = response[0][2]
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
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'N', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
        WHERE CUST_NO = '{self.number}'
        """
        db.query_db(query, commit=True)
        create_customer_log(customer_number=self.number,
                            first_name=self.first_name,
                            last_name=self.last_name,
                            name=self.name,
                            phone_1=self.phone_1,
                            status_1_col_name="unsubscribed",
                            status_1_data=f"Unsubscribed on {datetime.now().strftime("%x")}",
                            log_location=creds.unsubscribed_sms)

    def subscribe_to_sms(self):
        query = f"""
        UPDATE AR_CUST
        SET INCLUDE_IN_MARKETING_MAILOUTS = 'Y', LST_MAINT_DT = '{str(datetime.now())[:-6] + "000"}'
        WHERE CUST_NO = '{self.number}'
        """
        db.query_db(query, commit=True)

    def get_total_spent(self, start_date, stop_date):
        pass


def get_customers_by_category(category):
    query = f"""
    SELECT CUST_NO
    FROM AR_CUST
    WHERE CATEG_COD = '{category}' AND EMAIL_ADRS_1 IS NOT NULL
    """
    db = QueryEngine()
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
