import json
import secrets
import string
from datetime import datetime, timezone
from email import utils
from database import Database as db
import requests

from setup import creds
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


class Coupon:
    def __init__(self, coupon_id):
        self.id = coupon_id
        self.name = ''
        self.type = ''
        self.amount = ''
        self.min_purchase = ''
        self.expires = ''
        self.enabled = ''
        self.code = ''
        self.applies_to = ''
        self.num_uses = ''
        self.max_uses = ''
        self.max_uses_per_customer = ''
        self.restricted_to = ''
        self.shipping_methods = ''
        self.date_created = ''
        self.get_coupon_details()

    def get_coupon_details(self):
        url = 'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons/{self.id}'
        headers = {
            'X-Auth-Token': creds.big_access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        response = requests.get(url=url, headers=headers)

        if response.status_code == 200:
            json_response = response.json()
            self.name = json_response['name']
            self.type = json_response['type']
            self.amount = json_response['amount']
            self.min_purchase = json_response['min_purchase']
            self.expires = json_response['expires']
            self.enabled = json_response['enabled']
            self.code = json_response['code']
            self.applies_to = json_response['applies_to']
            self.num_uses = json_response['num_uses']
            self.max_uses = json_response['max_uses']
            self.max_uses_per_customer = json_response['max_uses_per_customer']
            self.restricted_to = json_response['restricted_to']
            self.shipping_methods = json_response['shipping_methods']
            self.date_created = json_response['date_created']


def bc_get_all_coupons(pretty=False):
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons'

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    response = requests.get(url=url, headers=headers)
    json_response = response.json()
    if pretty:
        pretty = response.content
        pretty = json.loads(pretty)
        pretty = json.dumps(pretty, indent=4)
        return pretty
    return json_response


def bc_create_coupon(
    name, coupon_type, amount, min_purchase, code, max_uses_per_customer, max_uses, expiration, enabled=True
):
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons'

    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    payload = {
        'name': name,
        'type': coupon_type,
        'amount': str(amount),
        'min_purchase': min_purchase,
        'enabled': enabled,
        'code': code,
        'max_uses_per_customer': max_uses_per_customer,
        'max_uses': max_uses,
        'applies_to': {'entity': 'categories', 'ids': [0]},
        'expires': expiration,
    }
    response = requests.post(url=url, headers=headers, json=payload)
    return response.json()


def cp_create_coupon(code, description, amount, min_purchase, coupon_type='A', apply_to='H', store='B'):
    """Will create a coupon code in SQL Database.
    Code is the coupon code, Description is the description of the coupon, Coupon Type is the type of coupon,
    Coupon Types: Amount ('A'), Prompted Amount ('B'), Percentage ('P'), Prompted Percent ('Q')
    Amount is the amount of the coupon, Min Purchase is the minimum purchase amount for the coupon to be valid. Apply to
    is either 'H' for Document or 'L' for Line ('H' is default), Store is either 'B' for Both instore and online or 'I'
    for in-store only ('B' is default)"""

    top_id_query = 'SELECT MAX(DISC_ID) FROM PS_DISC_COD'
    response = db.query(top_id_query)
    if response is not None:
        top_id = response[0][0]
        top_id += 1
        query = f"""
        INSERT INTO PS_DISC_COD(DISC_ID, DISC_COD, DISC_DESCR, DISC_TYP, DISC_AMT, APPLY_TO, MIN_DISCNTBL_AMT, DISC_VAL_FOR)
        VALUES ('{top_id}', '{code}', '{description}', '{coupon_type}', '{amount}', '{apply_to}', '{min_purchase}', '{store}')
        """
        try:
            db.query(query)
        except Exception as e:
            error_handler.error_handler.add_error_v(error=f'CP Coupon Insertion Error: {e}', origin='coupons.py')
        else:
            error_handler.logger.success('CP Coupon Insertion Success!')

    else:
        error_handler.logger.info('Error: Could not create coupon')


def cp_delete_coupon(code):
    query = f"""
    DELETE FROM PS_DISC_COD WHERE DISC_COD = '{code}'
    """
    try:
        db.query(query)
    except Exception as e:
        error_handler.error_handler.add_error_v(error=f'CP Coupon Deletion Error: {e}', origin='cp_delete_coupon')
    else:
        error_handler.logger.success(f'Deleted Coupon: {code}')


def bc_delete_coupon(coupon_id):
    url = f'https://api.bigcommerce.com/stores/{creds.big_store_hash}/v2/coupons/{coupon_id}'
    headers = {
        'X-Auth-Token': creds.big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    return requests.delete(url=url, headers=headers)


def generate_random_code(length):
    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(length))
    return str(res)


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def delete_expired_coupons():
    error_handler.logger.info(f'Deleting Expired Coupons: Starting at {datetime.now():%H:%M:%S}')
    total = 0
    coupons = bc_get_all_coupons(pretty=False)
    current_time = datetime.now(timezone.utc)
    for x in coupons:
        coupon = Coupon(x['id'])
        expiration_date = coupon.expires
        if expiration_date != '':
            expiration_date = utils.parsedate_to_datetime(expiration_date)
            expiration_date = utc_to_local(expiration_date)
            if expiration_date < current_time:
                # Target Coupon for Deletion Found
                # Delete from Counterpoint
                cp_delete_coupon(coupon.code)
                # Delete from BigCommerce
                bc_delete_coupon(coupon.id)
                total += 1
                error_handler.logger.info(f'Deleted Coupon: {coupon.code}')

    error_handler.logger.info(f'Deleting Expired Coupons: Finished at {datetime.now():%H:%M:%S}')
    error_handler.logger.info(f'Total Coupons Deleted: {total}')
