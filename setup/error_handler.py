from datetime import datetime
from setup import creds


class Logger:
    def __init__(self, log_file: str):
        self.log_file = log_file

        if not self.log_file.endswith('.log'):
            self.log_file += '.log'

    def update_log_file(self):
        # Replace the last part of old log file with the new log file
        new_ending = f'{datetime.now():%m_%d_%y}.log'
        self.log_file = f'{'_'.join(self.log_file.split('_')[0:-3])}_{new_ending}'

    def header(self, message: str):
        self.update_log_file()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        template = f'[{timestamp}] {message}'
        self.log('------------------')
        self.log(template)
        self.log('------------------')
        print(template)

    def log(self, message: str):
        self.update_log_file()
        with open(self.log_file, 'a') as file:
            file.write(f'{message}\n')

    def success(self, message: str):
        self.update_log_file()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        template = f'[SUCCESS] [{timestamp}] {message}'

        self.log(template)
        print(template)

    def info(self, message: str):
        self.update_log_file()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        template = f'[INFO] [{timestamp}] {message}'

        self.log(template)
        print(template)

    def warn(self, message: str):
        self.update_log_file()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        template = f'[WARNING] [{timestamp}] {message}'

        self.log(template)
        print(template)


class ErrorHandler:
    def __init__(self, logger: Logger = None):
        self.errors = []
        self.logger = logger

    def add_error(self, error: str, origin: str = None, type: str = 'ERROR', traceback=None):
        err = self.Error(message=error, origin=origin, type=type, traceback=traceback)
        self.errors.append(err)
        return err

    def add_error_v(self, error: str, origin: str = None, type: str = 'ERROR', traceback=None):
        err = self.add_error(error, origin=origin, type=type, traceback=traceback)
        self.logger.log(
            str(err)
        )  # Added for verbose logging in server applications where print_errors is not called
        print(err)

    def print_errors(self):
        if self.logger:
            self.logger.log('')
            self.logger.log('ERRORS:')
            self.logger.log('------------------------------')

        if self.errors:
            for error in self.errors:
                print(error)
                if self.logger:
                    self.logger.log(str(error))
        else:
            self.logger.log('No Sync Errors Found.')

        if self.logger:
            self.logger.log('------------------------------')
            self.logger.log('')
            self.logger.log('')

    class Error:
        def __init__(self, message: str, origin: str = None, type: str = 'ERROR', traceback=None):
            self.message = message
            self.origin = origin
            self.timestamp = datetime.now()
            self.type = type
            self.traceback = traceback

        def __str__(self):
            timestamp_str = self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            origin_str = f' [{self.origin}] ' if self.origin else ' '

            prefix = f'[{self.type}]{origin_str}[{timestamp_str}]'

            return f'{prefix} {self.message} {self.traceback if self.traceback else ""}'


class GlobalErrorHandler:
    """General Logging for the entire application"""

    logger = Logger(f"{creds.global_log}/log_{datetime.now().strftime("%m_%d_%y")}.log")
    error_handler = ErrorHandler(logger)


class ScheduledTasksErrorHandler:
    """General Logging for the entire application"""

    logger = Logger(creds.scheduled_tasks_log)
    error_handler = ErrorHandler(logger)


class SMSErrorHandler:
    """Logging for sms texts"""

    logger = Logger(creds.sms_log)
    error_handler = ErrorHandler(logger)


class SMSEventHandler:
    """Logging for SMS Unsubscribes, Landline Handling, and other SMS Events"""

    logger = Logger(creds.sms_event_log)
    error_handler = ErrorHandler(logger)


class ProcessInErrorHandler:
    """Logging for the Process In Integration"""

    logger = Logger(creds.process_in_log)
    error_handler = ErrorHandler(logger)


class ProcessOutErrorHandler:
    """Logging for the Process Out Integration"""

    logger = Logger(creds.process_out_log)
    error_handler = ErrorHandler(logger)


class LeadFormErrorHandler:
    """Logging for the Design Lead Form"""

    logger = Logger(creds.design_lead_log)
    error_handler = ErrorHandler(logger)


if __name__ == '__main__':
    # logger = GlobalErrorHandler.logger
    # error_handler = GlobalErrorHandler.error_handler

    # error_handler.add_error_v('This is an error message')
    # error_handler.add_error_v('This is a warning message', type='WARNING')

    # logger.success('This is a success message')
    # logger.warn('This is a warning message')
    # logger.info('This is an info message')

    # error_handler.print_errors()
    pass
