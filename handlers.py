import traceback
import functools


def error_handler(logger):
    def decorator(f):
        @functools.wraps(f)
        def wrapper_func(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                logger.error(traceback.format_exc())
        return wrapper_func
    return decorator
