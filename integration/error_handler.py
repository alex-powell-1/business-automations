import datetime

class Logger:
    def __init__(self, log_file: str):
        self.log_file = log_file

        if not self.log_file.endswith(".log"):
            self.log_file += ".log"

    def log(self, message: str):
        with open(self.log_file, "a") as file:
            file.write(f"{message}\n")

    def success(self, message: str):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        template = f"[SUCCESS] [{timestamp}] {message}"

        self.log(template)
        print(template)

    def info(self, message: str):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        template = f"[INFO] [{timestamp}] {message}"

        self.log(template)
        print(template)

    def warn(self, message: str):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        template = f"[WARNING] [{timestamp}] {message}"

        self.log(template)
        print(template)

class ErrorHandler:
    def __init__(self, logger: Logger = None):
        self.errors = []
        self.logger = logger

    def add_error(self, error: str, origin: str = None, type: str = None):
        err = self.Error(error, origin, type)
        self.errors.append(err)
        return err

    def add_error_v(self, error: str, origin: str = None, type: str = None):
        err = self.add_error(error, origin, type)
        print(err)

    def print_errors(self):
        if self.logger:
            self.logger.log("")
            self.logger.log("ERRORS:")
            self.logger.log("------------------")

        for error in self.errors:
            print(error)
            if self.logger:
                self.logger.log(str(error))
    
    class Error:
        def __init__(self, message: str, origin: str = None, type: str = "ERROR"):
            self.message = message
            self.origin = origin
            self.timestamp = datetime.datetime.now()
            self.type = type

        def __str__(self):
            timestamp_str = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            origin_str = f" [{self.origin}] " if self.origin else " "

            prefix = f"[{self.type}]{origin_str}[{timestamp_str}]"

            return f"{prefix} {self.message}"
        
class GlobalErrorHandler:
    logger = Logger("//mainserver/CPSQL.1/business_automations/integration.log")
    error_handler = ErrorHandler(logger)


if __name__ == "__main__":
    logger = GlobalErrorHandler.logger
    error_handler = GlobalErrorHandler.error_handler

    error_handler.add_error_v("This is an error message")
    error_handler.add_error_v("This is a warning message", type="WARNING")

    logger.success("This is a success message")
    logger.warn("This is a warning message")
    logger.info("This is an info message")

    error_handler.print_errors()