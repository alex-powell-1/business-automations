import requests

import integration.utilities as utilities
from integration.database import Database
from setup import creds

class GiftCertificates:
    def __init__(self, last_sync):
        self.last_sync = last_sync
        self.db = Database.db

    def get_certificates(self):
        query = f"""
        SELECT CUST_NO, FST_NAM, LST_NAM, EMAIL_ADRS_1, PHONE_1, LOY_PTS_BAL, ADRS_1, CITY, STATE, ZIP_COD, CNTRY
        FROM AR_CUST
        WHERE
        LST_MAINT_DT > '{self.last_sync}' and
        CUST_NAM_TYP = 'P'
        """
        
        response = self.db.query_db(query)
        if response is not None:
            result = []
            for x in response:
                if x is not None:
                    result.append(self.Customer(x))
            return result