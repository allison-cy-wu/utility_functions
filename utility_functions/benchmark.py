import time
import functools


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        return_value = func(*args, **kwargs)
        end_time = time.time()
        print(f"Total time taken in {func.__name__}: {end_time - start_time} seconds.")
        return return_value

    return wrapper

