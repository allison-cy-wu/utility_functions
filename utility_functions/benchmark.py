import time
import functools
import datetime


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        return_value = func(*args, **kwargs)
        end_time = time.time()
        secs_elapsed = end_time - start_time
        time_elapsed = str(datetime.timedelta(seconds = secs_elapsed)).split(':')

        if int(time_elapsed[0]) > 0:
            print(f"Total time taken in {func.__name__}: "
                  f"{int(time_elapsed[0])} hours "
                  f"{int(time_elapsed[1])} minutes "
                  f"{float(time_elapsed[2])} seconds.")
        elif int(time_elapsed[1]) > 0:
            print(f"Total time taken in {func.__name__}: "
                  f"{int(time_elapsed[1])} minutes "
                  f"{float(time_elapsed[2])} seconds.")
        else:
            print(f"Total time taken in {func.__name__}: {secs_elapsed} seconds.")

        return return_value

    return wrapper

