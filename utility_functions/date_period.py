from datetime import datetime, timedelta


def date_period(period: int, start_date: str = 0):
    """
    Author: Allison Wu
    Description: A function that returns end_date = start_date + period (in days).
    :param period: can be either positive or negative integers to specify the period
    :param start_date: has to be in YYYYMMDD format
    :return: end_date: in YYYYMMDD format
    """

    # TODO Check the stat_date format - Has to be YYYYMMDD
    if start_date == 0:
        start_date = datetime.now()
    else:
        start_date = datetime.strptime(start_date, '%Y%m%d')

    end_date: str = datetime.strftime(start_date + timedelta(days=period), '%Y%m%d')
    return end_date
