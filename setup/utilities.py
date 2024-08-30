import json
import os
import re
import time
from setup import creds
from datetime import datetime
from email.utils import formatdate
import base64
from setup.error_handler import ProcessOutErrorHandler
from PIL import Image, ImageOps, ImageDraw, ImageFont


class PhoneNumber:
    def __init__(self, phone_number: str):
        self.raw = phone_number
        if not PhoneNumber.is_valid(self.raw):
            raise ValueError(f'Invalid phone number format. Input: {phone_number}')
        self.stripped = PhoneNumber.strip_number(phone_number)
        self.area_code = self.stripped[0:3]
        self.exchange = self.stripped[3:6]
        self.subscriber_number = self.stripped[6:]

    def __str__(self):
        return f'({self.area_code}) {self.exchange}-{self.subscriber_number}'

    @staticmethod
    def is_valid(phone_number) -> bool:
        """Validates a phone number using regex."""
        pattern = r'(\+\d{1,3})?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        if re.match(pattern, phone_number):
            return True

    @staticmethod
    def strip_number(phone_number: str):
        return (
            phone_number.replace('+1', '')
            .replace('-', '')
            .replace('(', '')
            .replace(')', '')
            .replace(' ', '')
            .replace('_', '')
        )

    def to_cp(self):
        return f'{self.area_code}-{self.exchange}-{self.subscriber_number}'

    def to_twilio(self):
        return f'+1{self.area_code}{self.exchange}{self.subscriber_number}'


class EmailAddress:
    @staticmethod
    def is_valid(email: str) -> bool:
        """Validates an email address using regex."""
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        if re.match(pattern, email):
            return True
        return False


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


from datetime import datetime
import pytz


def convert_utc_to_local(utc_dt):
    """
    Convert a UTC datetime to local time.

    :param utc_dt: The UTC datetime object.
    :param local_tz: The local timezone as a string (e.g., 'America/New_York').
    :return: The local datetime object.
    """
    try:
        # Define the UTC timezone
        utc = pytz.utc

        # Convert the datetime to UTC
        utc_dt = datetime.strptime(utc_dt, '%Y-%m-%dT%H:%M:%SZ')
        utc_dt = utc.localize(utc_dt)

        # Define the local timezone
        local_timezone = pytz.timezone('America/New_York')

        # Convert the datetime to the local timezone
        local_dt = utc_dt.astimezone(local_timezone)

        # Remove the timezone information
        local_naive_dt = local_dt.replace(tzinfo=None)

        return local_naive_dt

    except:
        return None


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


if '__main__' == __name__:
    # Example usage
    phone = PhoneNumber('828-234-1265').to_twilio()
    print(phone)


def combine_images(
    product_image_path,
    barcode_image_path,
    combined_image_path=None,
    padding=100,
    barcode_text=None,
    expires_text=None,
):
    # Open the product and barcode images
    product_image = Image.open(product_image_path)
    barcode_image = Image.open(barcode_image_path)

    # Get the width of the wider image
    target_width = max(product_image.width, barcode_image.width)

    # Scale images to have the same width
    if product_image.width != target_width:
        product_image = product_image.resize(
            (target_width, int(product_image.height * target_width / product_image.width))
        )
    if barcode_image.width != target_width:
        barcode_image = barcode_image.resize(
            (target_width, int(barcode_image.height * target_width / barcode_image.width))
        )

    target_width = target_width + 2 * padding

    # Add padding around the images
    product_image = ImageOps.expand(product_image, border=padding, fill='white')
    barcode_image = ImageOps.expand(barcode_image, border=padding, fill='white')

    # Calculate combined height with margin between images
    combined_height = product_image.height + barcode_image.height

    global curr_height
    curr_height = combined_height

    def add_text(text, size):
        size *= 2

        def textsize(text, font):
            im = Image.new(mode='P', size=(0, 0))
            draw = ImageDraw.Draw(im)
            _, _, width, height = draw.textbbox((0, 0), text=text, font=font)
            return width, height

        # Add small centered text under the barcode
        # draw = ImageDraw.Draw(combined_image)
        font = ImageFont.load_default()
        font = font.font_variant(size=size)
        text = text  # Replace with desired text
        text_width, text_height = textsize(text, font=font)
        text_x = (target_width - text_width) // 2

        global curr_height
        text_y = curr_height
        curr_height = curr_height + text_height + (padding // 2)

        def draw_text(fill='black'):
            draw = ImageDraw.Draw(combined_image)
            draw.text((text_x, text_y), text, font=font, fill=fill)

        return draw_text

    if barcode_text is not None:
        draw_code_text = add_text(barcode_text, size=64)

    if expires_text is not None:
        draw_expires_text = add_text(expires_text, size=50)

    # Create a new image for combined output
    combined_image = Image.new('RGB', (target_width, curr_height + padding // 2), 'white')

    # Paste product image and barcode image into the combined image
    combined_image.paste(product_image, (0, 0))
    combined_image.paste(barcode_image, (0, product_image.height))

    if barcode_text is not None:
        draw_code_text()

    if expires_text is not None:
        draw_expires_text()

    if combined_image_path is not None:
        combined_image.save(combined_image_path)

    return combined_image
