import json
import os
from integration.database import query_engine
import re
import time
from setup import creds
from datetime import datetime
from email.utils import formatdate
import base64
from setup.error_handler import ProcessOutErrorHandler


def parse_custom_url(string: str):
    """Uses regular expression to parse a string into a URL-friendly format."""
    return '-'.join(str(re.sub('[^A-Za-z0-9 ]+', '', string)).lower().split(' '))


def get_filesize(filepath):
    try:
        file_size = os.path.getsize(filepath)
    except FileNotFoundError:
        return None
    else:
        return file_size


def get_product_images():
    ProcessOutErrorHandler.logger.info('Getting product images.')
    product_images = []
    # Iterate over all files in the directory
    for filename in os.listdir(creds.photo_path):
        if filename not in ['Thumbs.db', 'desktop.ini', '.DS_Store']:
            # filter out trailing filenames
            if '^' in filename:
                if filename.split('.')[0].split('^')[1].isdigit():
                    product_images.append([filename, os.path.getsize(f'{creds.photo_path}/{filename}')])
            else:
                product_images.append([filename, os.path.getsize(f'{creds.photo_path}/{filename}')])

    ProcessOutErrorHandler.logger.info(f'Found {len(product_images)} images.')
    return product_images


def timer(func):
    """Decorator function to time the execution of a function."""

    def wrapper_function(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print(f'{time.time() - start_time} seconds.')
        return result

    return wrapper_function


def convert_to_rfc2822(date: datetime):
    return formatdate(int(date.timestamp()))


def convert_to_iso8601(date: datetime, add_tz=True):
    return date.isoformat()


def convert_to_utc(date: datetime):
    return date.astimezone().isoformat()


def make_datetime(date_string):
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')


def country_to_country_code(country):
    country_codes = {'United States': 'US', 'Canada': 'CA', 'Mexico': 'MX', 'United Kingdom': 'GB'}

    return country_codes[country] if country in country_codes else country


def convert_timezone(timestamp, from_zone, to_zone):
    """Convert from UTC to Local Time"""
    start_time = timestamp.replace(tzinfo=from_zone)
    result_time = start_time.astimezone(to_zone).strftime('%Y-%m-%d %H:%M:%S')
    return result_time


def pretty_print(response):
    """Takes in a JSON object and returns an indented"""
    print(json.dumps(response, indent=4))


def get_all_binding_ids():
    """Returns a list of unique and validated binding IDs from the IM_ITEM table."""

    response = query_engine.QueryEngine.query(
        'SELECT DISTINCT USR_PROF_ALPHA_16 '
        "FROM IM_ITEM WHERE IS_ECOMM_ITEM = 'Y'"
        'AND USR_PROF_ALPHA_16 IS NOT NULL'
    )

    def valid(binding_id):
        return re.match(creds.binding_id_format, binding_id)

    return [binding[0] for binding in response if valid(binding[0])]


def encode_base64(input_string):
    # Ensure the string is in bytes, then encode it
    encoded_string = base64.b64encode(input_string.encode())
    # Convert the bytes back into a string and return it
    return encoded_string.decode()


def get_last_sync(file_name='last_sync.txt'):
    """Read the last sync time from a file for use in sync operations."""
    with open(file_name, 'r+') as file:
        last_sync = datetime.strptime(file.read(), '%Y-%m-%d %H:%M:%S')
        return last_sync


def set_last_sync(file_name, start_time):
    """Write the last sync time to a file for future use."""
    with open(file_name, 'w') as file:
        file.write(start_time.strftime('%Y-%m-%d %H:%M:%S'))


class VirtualRateLimiter:
    is_rate_limited = False
    limited_until = None
    request_quota = 140
    request_time = 30

    requests = []

    @staticmethod
    def pause_requests(seconds_to_wait: float = 0, silent: bool = False):
        VirtualRateLimiter.is_rate_limited = True
        VirtualRateLimiter.limited_until = time.time() + seconds_to_wait
        if not silent:
            ProcessOutErrorHandler.logger.warn(
                f'Rate limit reached. Pausing requests for {seconds_to_wait} seconds.'
            )

    @staticmethod
    def is_paused():
        if VirtualRateLimiter.is_rate_limited:
            if time.time() >= VirtualRateLimiter.limited_until:
                VirtualRateLimiter.is_rate_limited = False
                return False
            else:
                return True
        else:
            return False

    @staticmethod
    def limit():
        VirtualRateLimiter.requests.append(time.time())

        sleep0 = (VirtualRateLimiter.request_time / VirtualRateLimiter.request_quota) * 5
        sleep1 = (VirtualRateLimiter.request_time / VirtualRateLimiter.request_quota) * 3
        sleep2 = sleep1 / 2
        sleep3 = sleep2 / 2

        if len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota:
            time_passed = time.time() - VirtualRateLimiter.requests.pop(0)
            if time_passed < VirtualRateLimiter.request_time:
                VirtualRateLimiter.pause_requests(VirtualRateLimiter.request_time * 1.2)

                VirtualRateLimiter.requests = []

                return True
            else:
                return False
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.75:
            time.sleep(sleep0)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.65:
            time.sleep(sleep1)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.45:
            time.sleep(sleep2)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.15:
            time.sleep(sleep3)

        while (time.time() - VirtualRateLimiter.requests[0]) > VirtualRateLimiter.request_time:
            VirtualRateLimiter.requests.pop(0)

    @staticmethod
    def wait():
        sleep0 = (VirtualRateLimiter.request_time / VirtualRateLimiter.request_quota) * 5
        sleep1 = (VirtualRateLimiter.request_time / VirtualRateLimiter.request_quota) * 3
        sleep2 = sleep1 / 2
        sleep3 = sleep2 / 2

        if len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.75:
            time.sleep(sleep0)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.65:
            time.sleep(sleep1)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.45:
            time.sleep(sleep2)
        elif len(VirtualRateLimiter.requests) > VirtualRateLimiter.request_quota * 0.15:
            time.sleep(sleep3)
