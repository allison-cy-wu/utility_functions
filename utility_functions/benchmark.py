import time
import functools
import datetime


def timer(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        return_value = func(*args, **kwargs)
        end_time = time.time()
        time_elapsed = str(datetime.timedelta(seconds = end_time - start_time)).split(':')

        if int(time_elapsed[0]) > 0:
            print(f"Total time taken in {func.__name__}: "
                  f"{time_elapsed[0]} hours "
                  f"{time_elapsed[2]} minutes "
                  f"{time_elapsed[3]} seconds.")
        elif int(time_elapsed[1]) > 0:
            print(f"Total time taken in {func.__name__}: "
                  f"{time_elapsed[2]} minutes "
                  f"{time_elapsed[3]} seconds.")
        else:
            print(f"Total time taken in {func.__name__}: "
                  f"{time_elapsed[3]} seconds.")

        return return_value

    return wrapper

