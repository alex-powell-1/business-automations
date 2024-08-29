from setup import creds
from setup.creds import Table
from setup.query_engine import QueryEngine as db
from setup.error_handler import ProcessInErrorHandler


class QR:
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def insert(qr_code, url, publication, medium, offer, description, coupon_code):
        query = f"""
        INSERT INTO {Table.qr} (QR_CODE, URL, PUBLICATION, MEDIUM, OFFER, DESCR, COUPON_CODE)
        Values('{qr_code}','{url}', '{publication}', '{medium}', '{offer}', '{description}', '{coupon_code}')
        """
        response = db.query(query)
        if response['code'] == 200:
            QR.logger.success(f'QR Code {qr_code} inserted successfully')
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to insert QR Code {qr_code}')
            return False

    def is_valid(qr_code):
        query = f"""
        SELECT ID FROM {Table.qr}
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        return len(response) > 0 if response else False

    def get_visit_count(qr_code):
        query = f"""
        SELECT VISIT_COUNT FROM {Table.qr}
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        return response[0][0] if response else 0

    def get_url(qr_code):
        query = f"""
        SELECT URL FROM {Table.qr}
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        return response[0][0] if response else None

    def visit(qr_code):
        if not QR.is_valid(qr_code):
            QR.error_handler.add_error_v(f'QR Code {qr_code} is invalid')
            return False

        query = f"""
        INSERT INTO {Table.qr_activity}(CODE, SCAN_DT)
        VALUES ('{qr_code}', GETDATE())
        
        UPDATE {Table.qr}
        SET VISIT_COUNT = VISIT_COUNT + 1, LST_SCAN = GETDATE()
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        if response['code'] == 200:
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to log QR Code visit{qr_code}')
            return False

    def delete(qr_code):
        query = f"""
        DELETE FROM {Table.qr}
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        if response['code'] == 200:
            QR.logger.success(f'QR Code {qr_code} deleted successfully')
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to delete QR Code {qr_code}')
            return False


if __name__ == '__main__':
    QR.get_url('123456')
