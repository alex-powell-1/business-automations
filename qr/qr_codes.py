from setup.creds import Table
from setup import creds
from database import Database as db
from setup.error_handler import ProcessInErrorHandler
import qrcode


class QR:
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def __init__(self, filename, query_params, url, publication, medium, description, offer=None, coupon_code=None):
        self.filename = filename
        self.qr_code = query_params
        self.url = url
        self.publication = publication
        self.medium = medium
        self.description = description
        self.offer = offer
        self.coupon_code = coupon_code

    def generate(self):
        qr = qrcode.make(f'{self.url}?qr={self.qr_code}')
        type(qr)  # qrcode.image.pil.PilImage
        qr.save(f'{creds.Company.public_files_local_path}/qr/{self.filename}.png')
        QR.insert(
            qr_code=self.qr_code,
            url=self.url,
            publication=self.publication,
            medium=self.medium,
            description=self.description,
            offer=self.offer,
            coupon_code=self.coupon_code,
        )

    @staticmethod
    def insert(qr_code, url, publication, medium, description, offer=None, coupon_code=None):
        optional_args = ''
        optional_values = ''
        if offer:
            optional_args += ', OFFER'
            optional_values = f", '{offer}'"
        if coupon_code:
            optional_args += ', COUPON_CODE'
            optional_values = f", '{coupon_code}'"

        query = f"""
        INSERT INTO {Table.qr} (QR_CODE, URL, PUBLICATION, MEDIUM, DESCR {optional_args})
        Values('{qr_code}','{url}', '{publication}', '{medium}', '{description}' {optional_values})
        """

        response = db.query(query)
        if response['code'] == 200:
            QR.logger.success(f'QR Code {qr_code} inserted successfully')
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to insert QR Code {qr_code}')
            return False

    @staticmethod
    def is_valid(qr_code):
        query = f"""
        SELECT ID FROM {Table.qr}
        WHERE QR_CODE = '{qr_code}'
        """
        response = db.query(query)
        return len(response) > 0 if response else False

    @staticmethod
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
    code = QR(
        filename='hickory-living',
        query_params='hkylv',
        url='https://settlemyrenursery.com/pages/landscape-design',
        publication='magazine',
        medium='print',
        description='Landscape Design Ad with House',
    )

    code.generate()
