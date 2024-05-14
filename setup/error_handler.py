from datetime import datetime
from setup import creds


def error_handler(title, log_file, error_count):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_count[0] += 1
                print(f"Error: {title}", file=log_file)
                print(e, file=log_file)
        return wrapper
    return decorator
