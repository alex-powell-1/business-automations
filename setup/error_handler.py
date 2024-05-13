def error_handler(func):
    def inner_function(log_file, title, error_count, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as err:
            print(f"Error: {title}", file=log_file)
            print(err, file=log_file)
            print("-----------------------\n", file=log_file)
            error_count += 1

    return inner_function
