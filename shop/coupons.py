import secrets
import string
from datetime import datetime, timezone

from setup.error_handler import ScheduledTasksErrorHandler as error_handler

from integration.shopify_api import Shopify

from integration.database import Database


def utc_to_local(utc_dt: datetime):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def local_to_utc(local_dt: datetime):
    return local_dt.astimezone(tz=timezone.utc)


def shopify_create_coupon(
    name,
    amount,
    min_purchase,
    code,
    max_uses,
    expiration,
    product_variants_to_add=[],
    product_variants_to_remove=[],
    products_to_add=[],
    products_to_remove=[],
    enabled=True,
):
    return Shopify.Discount.Code.Basic.create(
        {
            'basicCodeDiscount': {
                'appliesOncePerCustomer': True,
                'code': code,
                'combinesWith': {'orderDiscounts': False, 'productDiscounts': False, 'shippingDiscounts': False},
                'customerGets': {
                    'items': {
                        'all': True,
                        'products': {
                            'productVariantsToAdd': product_variants_to_add,
                            'productVariantsToRemove': product_variants_to_remove,
                            'productsToAdd': products_to_add,
                            'productsToRemove': products_to_remove,
                        },
                    },
                    'value': {'discountAmount': {'amount': amount, 'appliesOnEachItem': False}},
                },
                'customerSelection': {'all': True},
                'endsAt': local_to_utc(expiration).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'minimumRequirement': {'subtotal': {'greaterThanOrEqualToSubtotal': min_purchase}},
                'startsAt': local_to_utc(datetime.now()).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'title': name,
                'usageLimit': max_uses,
            }
        },
        eh=error_handler,
    )


def cp_has_coupon(code):
    query = f"""
    SELECT COUNT(*) FROM PS_DISC_COD WHERE DISC_COD = '{code}'
    """
    try:
        response = Database.query(query)
        if response is not None:
            return int(response[0][0]) > 0

        return False
    except Exception as e:
        error_handler.error_handler.add_error_v(
            error=f'CP Coupon Check Error: {e}\nCode: {code}', origin='coupons.py'
        )
        return False


def cp_create_coupon(code, description, amount, min_purchase, coupon_type='A', apply_to='H', store='B'):
    """Will create a coupon code in SQL Database.
    Code is the coupon code, Description is the description of the coupon, Coupon Type is the type of coupon,
    Coupon Types: Amount ('A'), Prompted Amount ('B'), Percentage ('P'), Prompted Percent ('Q')
    Amount is the amount of the coupon, Min Purchase is the minimum purchase amount for the coupon to be valid. Apply to
    is either 'H' for Document or 'L' for Line ('H' is default), Store is either 'B' for Both instore and online or 'I'
    for in-store only ('B' is default)"""

    top_id_query = 'SELECT MAX(DISC_ID) FROM PS_DISC_COD'
    response = Database.query(top_id_query)
    top_id = None
    if response is not None:
        top_id = response[0][0]
        top_id += 1
        query = f"""
        INSERT INTO PS_DISC_COD(DISC_ID, DISC_COD, DISC_DESCR, DISC_TYP, DISC_AMT, APPLY_TO, MIN_DISCNTBL_AMT, DISC_VAL_FOR)
        VALUES ('{top_id}', '{code}', '{description}', '{coupon_type}', '{amount}', '{apply_to}', '{min_purchase}', '{store}')
        """
        try:
            Database.query(query)
        except Exception as e:
            error_handler.error_handler.add_error_v(error=f'CP Coupon Insertion Error: {e}', origin='coupons.py')
        else:
            error_handler.logger.success('CP Coupon Insertion Success!')

    else:
        error_handler.logger.info('Error: Could not create coupon')

    return top_id


def cp_deactivate_coupon(shop_id):
    query = f"""
    SELECT DISC_ID FROM VI_SHOP_DISC WHERE SHOP_ID = '{shop_id}'
    """

    try:
        response = Database.query(query)
        for row in response:
            disc_id = row[0]
            query = f"""
            UPDATE PS_DISC_COD
            SET MIN_DISCNTBL_AMT = 100000
            WHERE DISC_ID = '{disc_id}'
            """
            response = Database.query(query)
            if response['code'] == 200:
                error_handler.logger.success('Shopify Coupon Deactivated Successfully!')
            else:
                error_handler.error_handler.add_error_v(
                    'Error deactivating shopify coupon', origin='stock_notification.py'
                )
    except Exception as e:
        error_handler.error_handler.add_error_v(
            f'Error deactivating shopify coupon: {e}', origin='stock_notification.py'
        )


def cp_delete_coupon(code):
    query = f"""
    DELETE FROM PS_DISC_COD WHERE DISC_COD = '{code}'
    """
    try:
        response = Database.query(query)

        if response['code'] == 200:
            error_handler.logger.success(f'Deleted Coupon: {code}')
            return True
        else:
            error_handler.error_handler.add_error_v(f'Could not find coupon in CounterPoint: {code}')
            return False
    except Exception as e:
        error_handler.error_handler.add_error_v(error=f'CP Coupon Deletion Error: {e}', origin='cp_delete_coupon')


def generate_random_code(length):
    res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(length))

    return res


def generate_random_coupon(length=10):
    res = generate_random_code(length)

    if cp_has_coupon(res):
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
            cp_deactivate_coupon(id)
            total += 1
        except Exception as e:
            error_handler.error_handler.add_error_v(
                f'Error deactivating coupon: {e}', origin='delete_expired_coupons'
            )

    error_handler.logger.success(f'Expired Coupons Deactivated. {total} / {len(ids)}')
