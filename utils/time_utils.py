import time
from datetime import datetime
from timeit import default_timer
import logging
from utils.logging_utils import Logger

info_log = Logger(name=__name__, level=logging.INFO).return_logger()


def format_time_delta(end_time: datetime, start_time: datetime) -> str:
    run_time = str(end_time - start_time)
    return run_time.split(".")[0]


def time_it(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        end = datetime.now()
        run_time = format_time_delta(end, start)
        return result, run_time
    return wrapper
