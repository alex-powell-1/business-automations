import secrets
import string
from datetime import datetime, timezone
from setup.query_engine import QueryEngine as db

from setup import creds
from setup.error_handler import ScheduledTasksErrorHandler as error_handler


class Coupon:
    def shopify_create_coupon(
        name, coupon_type, amount, min_purchase, code, max_uses_per_customer, max_uses, expiration, enabled=True
    ):
        pass

    def cp_has_coupon(code):
        query = f"""
        SELECT COUNT(*) FROM PS_DISC_COD WHERE DISC_COD = '{code}'
        """
        try:
            response = db.query(query)
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
                error_handler.error_handler.add_error_v(
                    error=f'CP Coupon Insertion Error: {e}', origin='coupons.py'
                )
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
            error_handler.error_handler.add_error_v(
                error=f'CP Coupon Deletion Error: {e}', origin='cp_delete_coupon'
            )
        else:
            error_handler.logger.success(f'Deleted Coupon: {code}')

    def shopify_delete_coupon(coupon_id):
        pass

    def generate_random_code(length):
        res = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(length))

        if Coupon.cp_has_coupon(res):
            return Coupon.generate_random_code(length)

        return res

    def utc_to_local(utc_dt: datetime):
        return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)

    def delete_expired_coupons():
        error_handler.logger.info(f'Deleting Expired Coupons: Starting at {datetime.now():%H:%M:%S}')
        total = 0
        # coupons = bc_get_all_coupons(pretty=False)
        # current_time = datetime.now(timezone.utc)
        # for x in coupons:
        #     coupon = Coupon(x['id'])
        #     expiration_date = coupon.expires
        #     if expiration_date != '':
        #         expiration_date = utils.parsedate_to_datetime(expiration_date)
        #         expiration_date = utc_to_local(expiration_date)
        #         if expiration_date < current_time:
        #             # Target Coupon for Deletion Found
        #             # Delete from Counterpoint
        #             cp_delete_coupon(coupon.code)
        #             # Delete from BigCommerce
        #             bc_delete_coupon(coupon.id)
        #             total += 1
        #             deleted_coupon_log_file = open(creds.deleted_coupon_log, 'a')
        #             for y, z in x.items():
        #                 print(str(y), ': ', z, file=deleted_coupon_log_file)
        #                 print(str(y), ': ', z)

        error_handler.logger.info(f'Deleting Expired Coupons: Finished at {datetime.now():%H:%M:%S}')
        error_handler.logger.info(f'Total Coupons Deleted: {total}')
