import secrets
import string
from datetime import datetime

from setup.error_handler import ScheduledTasksErrorHandler as error_handler

from integration.shopify_api import Shopify
from setup.utilities import utc_to_local
from integration.database import Database


def generate_random_code(length):
    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(length))

    return res


def generate_random_coupon(length=10):
    res = generate_random_code(length)

    if Database.Counterpoint.Discount.has_coupon(res):
        return generate_random_coupon(length)

    return res


def get_expired_coupons():
    try:
        discounts = [x['node'] for x in Shopify.Discount.get()['discountNodes']['edges']]

        expired_discounts = []

        for discount in discounts:
            try:
                ends_at = discount['discount']['endsAt']
                if ends_at is None:
                    raise

                ends_at = datetime.strptime(ends_at, '%Y-%m-%dT%H:%M:%SZ')
                ends_at = utc_to_local(ends_at)

                if ends_at < datetime.now():
                    expired_discounts.append(discount)
            except:
                pass

        return expired_discounts
    except Exception as e:
        error_handler.error_handler.add_error_v(f'Error getting expired coupons: {e}', origin='get_expired_coupons')
        return []


def delete_expired_coupons():
    error_handler.logger.info('Deleting Expired Coupons')
    total = 0

    ids = [x['id'] for x in get_expired_coupons()]

    for id in ids:
        try:
            Shopify.Discount.Code.deactivate(id)
            Database.Counterpoint.Discount.deactivate(shop_id=id)
            total += 1
        except Exception as e:
            error_handler.error_handler.add_error_v(
                f'Error deactivating coupon: {e}', origin='delete_expired_coupons'
            )

    error_handler.logger.success(f'Expired Coupons Deactivated. {total} / {len(ids)}')
