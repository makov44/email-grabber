import traceback


def error_handler(logger):
    def decorator(f):
        def wrapper_func(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                logger.error(traceback.format_exc())
        return wrapper_func
    return decorator
