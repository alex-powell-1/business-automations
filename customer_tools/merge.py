from database import Database
from traceback import format_exc as tb
from setup.error_handler import ScheduledTasksErrorHandler


def get_duplicate_emails():
    """Returns a list of email addresses that are duplicated in the AR_CUST table. Excludes email addresses that are
    associated with open documents."""
    query = """
    SELECT EMAIL_ADRS_1
    FROM AR_CUST
    WHERE EMAIL_ADRS_1 != '' AND PHONE_1 NOT IN (SELECT EMAIL_ADRS_1 
							FROM PS_DOC_CONTACT 
							WHERE EMAIL_ADRS_1 IS NOT NULL and EMAIL_ADRS_1 != '')
    GROUP BY EMAIL_ADRS_1
    HAVING COUNT(EMAIL_ADRS_1) > 1"""
    response = Database.query(query)
    return [x[0] for x in response] if response is not None else None


def get_duplicate_phones():
    """Returns a list of phone numbers that are duplicated in the AR_CUST table. Excludes phone numbers that are
    associated with open documents."""
    query = """
    SELECT PHONE_1
    FROM AR_CUST
    WHERE PHONE_1 != '' AND PHONE_1 NOT IN (SELECT PHONE_1 
							                FROM PS_DOC_CONTACT 
							                WHERE PHONE_1 IS NOT NULL and PHONE_1 != '')
    GROUP BY PHONE_1
    HAVING COUNT(PHONE_1) > 1"""
    response = Database.query(query)
    return [x[0] for x in response] if response is not None else None


def get_duplicate_customers(email=False, phone=False) -> list:
    if not email and not phone:
        return 'Please select email or phone'
    result = []
    if email:
        duplicates_emails = get_duplicate_emails()
        if duplicates_emails is not None:
            for email in duplicates_emails:
                query = f"""
                SELECT CUST_NO
                FROM AR_CUST
                WHERE EMAIL_ADRS_1 = '{email}'
                """
                response = Database.query(query, mapped=True)
                if response is not None:
                    duplicate_accounts = []
                    for customer in response['data']:
                        duplicate_accounts.append(customer['CUST_NO'])
                    result.append(duplicate_accounts)
    if phone:
        duplicates_phones = get_duplicate_phones()
        if duplicates_phones is not None:
            for phone in duplicates_phones:
                query = f"""
                SELECT CUST_NO
                FROM AR_CUST
                WHERE PHONE_1 = '{phone}'
                """
                response = Database.query(query, mapped=True)
                if response is not None:
                    duplicate_accounts = []
                    for customer in response['data']:
                        duplicate_accounts.append(customer['CUST_NO'])
                    result.append(duplicate_accounts)

    # Check if there are
    return result


class Candidate:
    def __init__(self, cust_no):
        self.customer = Database.CP.Customer(cust_no)

    def __str__(self) -> str:
        return f'{self.customer.NAM} - {self.customer.CUST_NO} - Last Sale: {self.customer.LST_SAL_DAT} - Rewards Card: {self.customer.LOY_CARD_NO} - PT Balance: {self.customer.LOY_PTS_BAL}'


class Job:
    def __init__(self, customer_list: list, test_mode=False, eh=ScheduledTasksErrorHandler):
        self.test_mode = test_mode
        self.eh = eh
        self.error_handler = self.eh.error_handler
        self.logger = self.eh.logger
        self.customers = [Candidate(x) for x in customer_list]
        self.to_customer: Candidate = self.get_to_customer()
        self.from_customers: list[Candidate] = self.get_from_customers()
        self.combined_customer: Database.CP.Customer = self.combine_customer_data()
        self.is_valid: bool = self.validate()

    def __str__(self) -> str:
        result = 'Merge Candidates:\n'
        count = 1
        for x in self.customers:
            result += f'Candidate #{count}: {x}\n'
            count += 1
        result += '--------------------\n'
        result += f'\nMerge Into: {self.to_customer}\n'
        result += '\n--------------------\n'

        return result

    def get_to_customer(self):
        """Determine the customer to merge into"""
        # Get the customer with the most recent sale
        candidates = []

        for x in self.customers:
            if x.customer.LST_SAL_DAT is not None:
                candidates.append(x)

        if candidates:
            candidates.sort(key=lambda x: x.customer.LST_SAL_DAT, reverse=True)
            return candidates[0]
        else:
            # If no sales, get the customer with the most recent LST_MAINT_DT
            for x in self.customers:
                if x.customer.LST_MAINT_DT is not None:
                    candidates.append(x)

            if candidates:
                candidates.sort(key=lambda x: x.customer.LST_MAINT_DT, reverse=True)
                return candidates[0]

    def get_from_customers(self) -> list[Candidate]:
        """Get the customers to merge from"""
        result = []
        for x in self.customers:
            if x.customer.CUST_NO != self.to_customer.customer.CUST_NO:
                result.append(x)
        return result

    def combine_customer_data(self):
        result = self.to_customer.customer

        if not result.CONTCT_2:
            for x in self.from_customers:
                if result.NAM:
                    if x.customer.NAM != result.NAM:
                        result.CONTCT_2 = x.customer.NAM
                        break

        if not result.PHONE_2:
            for x in self.from_customers:
                if x.customer.PHONE_1 != result.PHONE_1:
                    result.PHONE_2 = x.customer.PHONE_1
                    break

        if not result.EMAIL_ADRS_2:
            for x in self.from_customers:
                if x.customer.EMAIL_ADRS_1:
                    if x.customer.EMAIL_ADRS_1 != result.EMAIL_ADRS_1:
                        result.EMAIL_ADRS_2 = x.customer.EMAIL_ADRS_1
                    break

        if not result.MBL_PHONE_2:
            for x in self.from_customers:
                if x.customer.MBL_PHONE_1:
                    if x.customer.MBL_PHONE_1 != result.MBL_PHONE_1:
                        result.MBL_PHONE_2 = x.customer.MBL_PHONE_1
                    break

        if result.NAM == 'Change Name':
            for x in self.from_customers:
                if x.customer.NAM != 'Change Name':
                    result.FST_NAM = x.customer.FST_NAM
                    result.LST_NAM = x.customer.LST_NAM
                    result.NAM = x.customer.NAM

        for x in self.from_customers:
            for key, value in x.customer.__dict__.items():
                if key == 'LOY_PTS_BAL':
                    result.LOY_PTS_BAL += value

                else:
                    if value is not None:
                        if result.__dict__[key] is None:
                            setattr(result, key, value)

        if result.EMAIL_ADRS_1 == result.EMAIL_ADRS_2:
            result.EMAIL_ADRS_2 = None

        if result.MBL_PHONE_1 == result.MBL_PHONE_2:
            result.MBL_PHONE_2 = None

        if result.PHONE_1 == result.PHONE_2:
            result.PHONE_2 = None

        if result.CONTCT_1 == result.CONTCT_2:
            result.CONTCT_2 = None

        return result

    def validate(self):
        # Validation #1: Check that the candidates are all the same customer type
        for x in self.customers:
            if x.customer.CUST_TYP != self.to_customer.customer.CUST_TYP:
                self.logger.info(
                    f'Invalid Merge Job: Customer Types do not match: {self.to_customer.customer.CUST_NO}: {self.to_customer.customer.CUST_TYP}, {x.customer.CUST_NO}: {x.customer.CUST_TYP}'
                )
                return False
        return True

    def merge(self):
        for x in self.from_customers:
            # Step 1: Merge Shipping Addresses
            Database.CP.Customer.ShippingAddress.merge(
                from_cust_no=x.customer.CUST_NO, to_cust_no=self.to_customer.customer.CUST_NO, eh=self.eh
            )

            # Step 2: Merge Customer Data
            Database.CP.Customer.merge_customer(
                from_cust_no=x.customer.CUST_NO, to_cust_no=self.to_customer.customer.CUST_NO, eh=self.eh
            )

        # Step 3: Update customer with combined data
        self.combined_customer.update()


class Merge:
    def __init__(self, test_mode=False, eh=ScheduledTasksErrorHandler):
        self.test_mode = test_mode
        self.eh = eh
        self.error_handler = self.eh.error_handler
        self.logger = self.eh.logger
        self.valid_jobs: list[Job] = []
        self.process()

    def process(self):
        self.logger.info('Starting Merge Process...')

        def get_phone_duplicates():
            # Get all duplicate customers by Phone
            phone_duplicates = get_duplicate_customers(phone=True)

            if phone_duplicates:
                for x in phone_duplicates:
                    job = Job(x, test_mode=self.test_mode, eh=self.eh)
                    if job.is_valid:
                        self.valid_jobs.append(job)

        def get_email_duplicates():
            # Get all duplicate customers by Email
            email_duplicates = get_duplicate_customers(email=True)
            if email_duplicates:
                for x in email_duplicates:
                    job = Job(x, test_mode=self.test_mode, eh=self.eh)
                    if job.is_valid:
                        self.valid_jobs.append(job)

        def process_valid_jobs():
            if self.valid_jobs:
                count = 1
                for job in self.valid_jobs:
                    try:
                        self.logger.info(f'Merge Job {count}/{len(self.valid_jobs)}\n{job}')
                        if self.test_mode:
                            for k, v in job.combined_customer.__dict__.items():
                                if k not in ['cust']:
                                    print(f'{k}: {v}')
                            inp = input('Continue? (y/n): ')
                            if inp.lower() == 'y':
                                job.merge()
                        else:
                            job.merge()

                    except Exception as e:
                        self.error_handler.add_error_v(
                            error=f'Error merging customers: {e}',
                            origin='Merge.py->Merge.process()',
                            traceback=tb(),
                        )
                    finally:
                        self.valid_jobs.remove(job)
                        count += 1

        get_phone_duplicates()
        process_valid_jobs()
        get_email_duplicates()
        process_valid_jobs()
        self.logger.info('Merge Process Complete.')


if __name__ == '__main__':
    merge = Merge()
