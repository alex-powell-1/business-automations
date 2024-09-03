import requests
from setup.error_handler import ProcessOutErrorHandler
from setup.utilities import VirtualRateLimiter
from time import sleep
from setup.creds import bc_api_headers


class BCRequests:
    """Handles requests to BigCommerce API with max retries and timeout and handling for status 429"""

    headers = bc_api_headers
    logger = ProcessOutErrorHandler.logger
    error_handler = ProcessOutErrorHandler.error_handler
    max_tries = 25
    timeout = 120

    @staticmethod
    def get(url):
        y = 1  # y is increased so that if there are more than y failed requests, operation stops
        rate_limited = True  # rate_limited is set to True so that the loop runs at least once
        while y <= BCRequests.max_tries and rate_limited:
            rate_limited = False
            try:
                response = requests.get(url=url, headers=BCRequests.headers, timeout=BCRequests.timeout)
                if response.status_code == 429:
                    rate_limited = True
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    sleep(seconds_to_wait)
                    y += 1
                    raise Exception('Rate Limited')
            except Exception as e:
                BCRequests.error_handler.add_error_v(
                    error=f'Error: {e}\n'
                    f'URL: {url}\n'
                    f'Response Code: {response.status_code}\n'
                    f'Response: {response.content}',
                    origin='PUT Request',
                )
                y += 1
            else:
                return response

        BCRequests.error_handler.add_error_v(error='Could not complete. Max Tries Reached.', origin='GET Request')

    @staticmethod
    def post(url, json=None, data=None):
        # Post request with max retries and timeout and handling for status 429
        y = 1
        rate_limited = True
        while y <= BCRequests.max_tries and rate_limited:
            rate_limited = False
            try:
                response = requests.post(
                    url=url, headers=BCRequests.headers, json=json, data=data, timeout=BCRequests.timeout
                )
                if response.status_code == 429:
                    rate_limited = True
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    sleep(seconds_to_wait)
                    y += 1
                    raise Exception('Rate Limited')
            except Exception as e:
                BCRequests.error_handler.add_error_v(
                    error=f'Error: {e}\n'
                    f'URL: {url}\n'
                    f'Response Code: {response.status_code}\n'
                    f'Response: {response.content}',
                    origin='POST Request',
                )
                y += 1
            else:
                return response

        BCRequests.error_handler.add_error_v(error='Could not complete. Max Tries Reached.', origin='POST Request')

    @staticmethod
    def put(url, json=None, data=None):
        # Put request with max retries and timeout and handling for status 429
        y = 1
        rate_limited = True
        while y <= BCRequests.max_tries and rate_limited:
            rate_limited = False
            try:
                response = requests.put(
                    url=url, json=json, data=data, headers=BCRequests.headers, timeout=BCRequests.timeout
                )
                if response.status_code == 429:
                    rate_limited = True
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    sleep(seconds_to_wait)
                    y += 1
                    raise Exception('Rate Limited')
            except Exception as e:
                BCRequests.error_handler.add_error_v(
                    error=f'Error: {e}\n'
                    f'URL: {url}\n'
                    f'Response Code: {response.status_code}\n'
                    f'Response: {response.content}',
                    origin='PUT Request',
                )
                y += 1
            else:
                return response

        BCRequests.error_handler.add_error_v(error='Could not complete. Max Tries Reached.', origin='PUT Request')

    @staticmethod
    def delete(url):
        # Delete request with max retries and timeout and handling for status 429
        y = 1
        rate_limited = True
        while y <= BCRequests.max_tries and rate_limited:
            rate_limited = False
            try:
                response = requests.delete(url, headers=BCRequests.headers, timeout=BCRequests.timeout)
                if response.status_code == 429:
                    rate_limited = True
                    ms_to_wait = int(response.headers['X-Rate-Limit-Time-Reset-Ms'])
                    seconds_to_wait = (ms_to_wait / 1000) + 1
                    VirtualRateLimiter.pause_requests(seconds_to_wait)
                    sleep(seconds_to_wait)
                    y += 1
                    raise Exception('Rate Limited')
            except Exception as e:
                BCRequests.error_handler.add_error_v(
                    error=f'Error: {e}\n'
                    f'URL: {url}\n'
                    f'Response Code: {response.status_code}\n'
                    f'Response: {response.content}',
                    origin='PUT Request',
                )
                y += 1
            else:
                return response
        BCRequests.error_handler.add_error_v(error='Could not complete. Max Tries Reached.', origin='DELETE Request')
