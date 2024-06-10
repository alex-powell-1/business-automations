import datetime

class Logger:
    def __init__(self, log_file: str):
        self.log_file = log_file

        if not self.log_file.endswith(".log"):
            self.log_file += ".log"

    def log(self, message: str):
        with open(self.log_file, "a") as file:
            file.write(f"{message}\n")

class ErrorHandler:
    def __init__(self, logger: Logger = None):
        self.errors = []
        self.logger = logger

    def add_error(self, error: str, origin: str = None, type: str = None):
        if type:
            self.errors.append(self.TypeError(error, origin, type))
        else:
            self.errors.append(self.Error(error, origin))

    def print_errors(self):
        for error in self.errors:
            print(error)
            if self.logger:
                self.logger.log(str(error))
    
    class Error:
        def __init__(self, message, author):
            self.message = message
            self.author = author
            self.timestamp = datetime.datetime.now()

        def __str__(self):
            timestamp_str = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            author_str = f"{self.author}: " if self.author else ""
            return f"[ERROR] [{timestamp_str}] {author_str}{self.message}"
        
    class TypeError (Error):
        def __init__(self, message, author, type):
            super().__init__(message, author)
            self.type = type

        def __str__(self):
            return f"{super().__str__()} ({self.type})"