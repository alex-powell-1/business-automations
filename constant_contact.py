import requests
import base64
import time
import json
from datetime import datetime, timedelta
from flask import Flask, request
from setup import creds
import webbrowser

import urllib.parse

from database import Database

from setup.error_handler import Logger, ErrorHandler

import multiprocessing

from promise import Promise

REDIRECT_URI = 'http://localhost:41022/auth'
REQUEST_TIMEOUT = 350

logger = Logger(log_file=f'{creds.log_main}/constant_contact.log')
error_handler = ErrorHandler(logger=logger)


def CUSTOMER_ADDED(name, email, error):
    return f'CUSTOMER_ADD | Name: {name} | Email: {email}'


def CUSTOMER_ERROR(name, email, error):
    return f'CUSTOMER_ERROR | Name: {name} | Email: {email or ""} | Error: {error}'


def EXECUTION_ERROR(name, email, error):
    return f'EXECUTION_ERROR | Error: {error}'


def NO_RESULTS(name, email, error):
    return 'NO_RESULTS | No results found'


def CUSTOMER_UPDATE(name, email, error):
    return f'CUSTOMER_UPDATE | Name: {name} | Email: {email}'


def AUTHORIZATION_REQUESTED(name, email, error):
    return 'AUTH_REQ | Authorization Requested'


def AUTHORIZATION_SUCCESS(name, email, error):
    return 'AUTH_SUCC | Authorization Success'


def AUTHORIZATION_ERROR(name, email, error):
    return f'AUTH_ERR | Error: {error}'


def REFRESH_REQUESTED(name, email, error):
    return 'REFRESH_REQ | Refresh Requested'


EVENTS = {
    'customerAdded': CUSTOMER_ADDED,
    'customerError': CUSTOMER_ERROR,
    'executionError': EXECUTION_ERROR,
    'noResults': NO_RESULTS,
    'customerUpdated': CUSTOMER_UPDATE,
    'authorizationRequested': AUTHORIZATION_REQUESTED,
    'authorizationSuccess': AUTHORIZATION_SUCCESS,
    'authorizationError': AUTHORIZATION_ERROR,
    'refreshRequested': REFRESH_REQUESTED,
}


def invoke_event(event, name=None, email=None, error=None):
    logger.log(EVENTS[event](name=name, email=email, error=error))


app = Flask(__name__)


@app.route('/access_token', methods=['GET'])
def access_token():
    print(request.args)
    return 'Access token endpoint'


def date_to_sql_date_string(date: datetime):
    return date.strftime('%Y-%m-%d 00:00:00')


def query_date(date: datetime):
    sale_date = date_to_sql_date_string(date)

    query = f"""
    SELECT FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, PROF_COD_2, PROF_COD_3 FROM AR_CUST
    WHERE LST_SAL_DAT = '{sale_date}'
    """

    rows = []

    response = Database.query(query)

    if response is not None:
        for row in response:
            rows.append(
                {
                    'FST_NAM': row[0],
                    'LST_NAM': row[1],
                    'EMAIL_ADRS_1': row[2],
                    'PHONE_1': row[3],
                    'LOY_PTS_BAL': row[4],
                    'PROF_COD_2': row[5],
                    'PROF_COD_3': row[6],
                }
            )

    return rows


class JSONdb:
    def __init__(self, filepath):
        self.filepath = filepath
        try:
            with open(filepath, 'r') as dbfile:
                self.data = json.load(dbfile)
        except FileNotFoundError:
            self.data = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.data[key] = value
        with open(self.filepath, 'w') as dbfile:
            json.dump(self.data, dbfile)

    def has(self, key):
        return key in self.data


db = JSONdb('persist.json')


def access_token_expired():
    try:
        expires = db.get('refresh', {}).get('expires')
    except:
        expires = 0
    return time.time() > expires - 1000 * 60 * 20


API_KEY = creds.constant_contact_api_key
CLIENT_SECRET = creds.constant_contact_client_secret


def refresh_access_token():
    invoke_event('refreshRequested')
    url = 'https://authz.constantcontact.com/oauth2/default/v1/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}

    url += f'?refresh_token={db.get("refresh", {"token": ""}).get("token")}&grant_type=refresh_token'

    bearer_token = base64.b64encode(f'{API_KEY}:{CLIENT_SECRET}'.encode()).decode()
    headers['Authorization'] = f'Basic {bearer_token}'

    response = requests.post(url, headers=headers)
    return response.json()


def get_authorized(app):
    def get_authorization(resolve, reject):
        invoke_event('authorizationRequested')

        @app.route('/auth')
        def authorize():
            if not request.args.get('code'):
                invoke_event('authorizationError', error='ConstantContact API did not return authorization code.')
                reject(None)
                return '', 400

            resolve(request.args.get('code'))

            invoke_event('authorizationSuccess')
            return '<script>window.close();</script>'

        # Open Authorization URL in browser
        webbrowser.open(get_authorization_url(API_KEY, REDIRECT_URI))

    return Promise(get_authorization)


def get_access_token(authorization_code):
    url = 'https://authz.constantcontact.com/oauth2/default/v1/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}

    url += f'?code={authorization_code}&redirect_uri={REDIRECT_URI}'

    bearer_token = base64.b64encode(f'{API_KEY}:{CLIENT_SECRET}'.encode()).decode()
    headers['Authorization'] = f'Basic {bearer_token}'

    response = requests.post(url, headers=headers)
    return response.json()


CUR_ACCESS_TOKEN = ''
TIME_OF_ACCESS = 0


def get_access_token_without_code(force_refresh=False):
    global CUR_ACCESS_TOKEN
    if db.has('refresh') and access_token_expired():
        data = refresh_access_token()
        access_token = data.get('access_token')
        refresh_token = data.get('refresh_token')
        expires_in = data.get('expires_in')
        db.set('refresh', {'access': access_token, 'token': refresh_token, 'expires': time.time() + expires_in})
        CUR_ACCESS_TOKEN = access_token
        return access_token

    if db.has('refresh') and not access_token_expired():
        CUR_ACCESS_TOKEN = db.get('refresh').get('access')
        return CUR_ACCESS_TOKEN

    if not force_refresh and CUR_ACCESS_TOKEN:
        return CUR_ACCESS_TOKEN

    authorization_code = get_authorized(app).get()
    if authorization_code is None:
        invoke_event('authorizationError', error='ConstantContact API did not return authorization code.')
        return None

    data = get_access_token(authorization_code)
    access_token = data.get('access_token')
    refresh_token = data.get('refresh_token')
    expires_in = data.get('expires_in')
    db.set('refresh', {'access': access_token, 'token': refresh_token, 'expires': time.time() + expires_in})
    CUR_ACCESS_TOKEN = access_token
    return access_token


API_URL = 'https://api.cc.email/v3/'


def get_url_for(endpoint):
    return f'{API_URL}{endpoint}'


def get_contact_lists():
    access_token = get_access_token_without_code()
    url = get_url_for('contact_lists')
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    response = requests.get(url, headers=headers).json()

    try:
        return response['lists']
    except:
        return []


LISTS = {}


def get_contact_list_id_from_name(name):
    if name in LISTS:
        return LISTS[name]
    lists = get_contact_lists()
    for list in lists:
        if list['name'] == name:
            LISTS[name] = list['list_id']
            return list['list_id']
    return None


def get_custom_fields():
    access_token = get_access_token_without_code()
    url = get_url_for('contact_custom_fields')
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    response = requests.get(url, headers=headers).json()
    try:
        return response['custom_fields']
    except:
        return []


FIELDS = {}


def get_custom_field_id_from_name(name):
    if name in FIELDS:
        return FIELDS[name]
    fields = get_custom_fields()
    for field in fields:
        if field['name'] == name:
            FIELDS[name] = field['custom_field_id']
            return field['custom_field_id']
    return None


def get_custom_fields_prop(fields):
    return [
        {'custom_field_id': get_custom_field_id_from_name(field['name']), 'value': field['value']}
        for field in fields
    ]


def build_constant_contact_request_body(customer_data, list_memberships=[]):
    custom_fields = get_custom_fields_prop([{'name': 'point_balance', 'value': customer_data['LOY_PTS_BAL']}])
    return json.dumps(
        {
            'email_address': customer_data['EMAIL_ADRS_1'],
            'first_name': customer_data['FST_NAM'],
            'last_name': customer_data['LST_NAM'],
            'phone_number': customer_data.get('PHONE_1'),
            'list_memberships': list_memberships,
            'custom_fields': custom_fields,
        }
    )


def get_authorization_url(client_id, redirect_uri, scope='contact_data offline_access', state=None):
    if state is None:
        state = base64.b64encode(str(time.time()).encode()).decode().replace('=', '')
    base_url = 'https://authz.constantcontact.com/oauth2/default/v1/authorize'
    params = {
        'client_id': client_id,
        'scope': scope,
        'response_type': 'code',
        'state': state,
        'redirect_uri': redirect_uri,
    }
    return f'{base_url}?{urllib.parse.urlencode(params)}'


def add_contact_to_constant_contact(customer_data, lists):
    access_token = get_access_token_without_code()
    list_ids = [get_contact_list_id_from_name(list_name) for list_name in lists]
    body = build_constant_contact_request_body(customer_data, list_ids)

    url = get_url_for('contacts/sign_up_form')
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }
    response = requests.post(url, headers=headers, data=body).json()
    time.sleep(REQUEST_TIMEOUT / 1000)
    return response


def handle_results(results):
    if not results:
        invoke_event('noResults')
        print('No Results Found')
        return

    def do_next(index=0):
        if index >= len(results):
            return
        try:
            data = add_contact_to_constant_contact(results[index], ['General Interest'])
            print('')
            print(f'Loading {index + 1} of {len(results)}')
            print(f'Estimated Time: {((len(results) - index) * REQUEST_TIMEOUT) / 1000:.0f} seconds')
            print(f'Estimated Completion: {datetime.now().strftime("%H:%M:%S")}')
            err_msg = data[0].get('error_message') or data.get('error_message') or 'Unknown error'
            if 'error_message' in data or ('action' in data and data['action'] == 'created'):
                invoke_event(
                    'customerError',
                    name=f"{results[index]['FST_NAM']} {results[index]['LST_NAM']}",
                    email=results[index]['EMAIL_ADRS_1'],
                    error=err_msg,
                )
            elif data['action'] == 'created':
                invoke_event(
                    'customerAdded',
                    name=f"{results[index]['FST_NAM']} {results[index]['LST_NAM']}",
                    email=results[index]['EMAIL_ADRS_1'],
                )
            elif data['action'] == 'updated':
                invoke_event(
                    'customerUpdated',
                    name=f"{results[index]['FST_NAM']} {results[index]['LST_NAM']}",
                    email=results[index]['EMAIL_ADRS_1'],
                )
            else:
                invoke_event(
                    'customerError',
                    name=f"{results[index]['FST_NAM']} {results[index]['LST_NAM']}",
                    email=results[index]['EMAIL_ADRS_1'],
                    error='Unknown error',
                )
        except Exception as error:
            invoke_event('executionError', error=error)
            print(error)

        do_next(index + 1)

    do_next()


def read_newsletter_data():
    rows = []
    # with open(r'\\mainserver\Share\logs\newsletter_signup.csv', 'r') as csvfile:
    #     reader = DictReader(csvfile)
    #     for row in reader:
    #         if datetime.fromisoformat(row['date']) > datetime.now() - timedelta(days=2):
    #             rows.append({'EMAIL_ADRS_1': row['email'], 'LOY_PTS_BAL': 0})

    query = f"""
    SELECT EMAIL FROM SN_NEWS
    WHERE CREATED_DT > '{datetime.now() - timedelta(days=2)}'
    """

    response = Database.query(query)

    if response is not None:
        for row in response:
            rows.append({'EMAIL_ADRS_1': row[0], 'LOY_PTS_BAL': 0})

    return rows


def test_add_function():
    response = add_contact_to_constant_contact(
        {
            'FST_NAM': 'Test',
            'LST_NAM': 'User',
            'EMAIL_ADRS_1': 'lukebbarrier04@outlook.com',
            'PHONE_1': '828-448-2576',
            'LOY_PTS_BAL': 0,
        },
        ['General Interest'],
    )
    print(response)


def run():
    app.run(port=41022)


def main():
    server = multiprocessing.Process(target=run)

    server.start()

    print("Processing yesterday's data.")
    time.sleep(2)
    handle_results(query_date(datetime.now() - timedelta(days=1)))

    print("Yesterday's data complete.")
    print("Processing today's data.")
    time.sleep(2)
    handle_results(query_date(datetime.now()))

    print("Today's data complete.")
    print('Processing newsletter data.')
    time.sleep(2)
    handle_results(read_newsletter_data())

    print('Newsletter data complete. Check logs for more information.')

    server.terminate()


if __name__ == '__main__':
    main()
