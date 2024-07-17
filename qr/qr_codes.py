from setup import creds
from setup import query_engine
from setup.error_handler import ProcessInErrorHandler


class QR:
    db = query_engine.QueryEngine()
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def insert(code, url):
        query = f"""
        INSERT INTO {creds.qr_table} (CODE, URL)
        VALUES ('{code}', '{url}')
        """
        response = QR.db.query_db(query, commit=True)
        if response['code'] == 200:
            QR.logger.success(f'QR Code {code} inserted successfully')
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to insert QR Code {code}')
            return False

    def is_valid(code):
        query = f"""
        SELECT ID FROM {creds.qr_table}
        WHERE CODE = '{code}'
        """
        response = QR.db.query_db(query)
        return len(response) > 0 if response else False

    def get_visit_count(code):
        query = f"""
        SELECT VISIT_COUNT FROM {creds.qr_table}
        WHERE CODE = '{code}'
        """
        response = QR.db.query_db(query)
        return response[0][0] if response else 0

    def get_url(code):
        query = f"""
        SELECT URL FROM {creds.qr_table}
        WHERE CODE = '{code}'
        """
        response = QR.db.query_db(query)
        return response[0][0] if response else None

    def visit(code):
        query = f"""
        INSERT INTO {creds.qr_activity_table}(CODE, SCAN_DT)
        VALUES ('{code}', GETDATE())
        
        UPDATE {creds.qr_table}
        SET VISIT_COUNT = VISIT_COUNT + 1, LST_SCAN = GETDATE()
        WHERE CODE = '{code}'
        """
        response = QR.db.query_db(query, commit=True)
        if response['code'] == 200:
            return QR.get_url(code)
        else:
            QR.error_handler.add_error_v(f'Failed to update QR Code {code}')
            return None

    def delete(code):
        query = f"""
        DELETE FROM {creds.qr_table}
        WHERE CODE = '{code}'
        """
        response = QR.db.query_db(query, commit=True)
        if response['code'] == 200:
            QR.logger.success(f'QR Code {code} deleted successfully')
            return True
        else:
            QR.error_handler.add_error_v(f'Failed to delete QR Code {code}')
            return False


print(QR.visit('123456'))
