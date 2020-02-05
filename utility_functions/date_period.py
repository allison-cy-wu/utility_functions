from datetime import datetime, timedelta
from typing import List, Tuple
from connect2Databricks.read2Databricks import redshift_ccg_read, redshift_cdw_read
from utility_functions.benchmark import timer
from utility_functions.custom_errors import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as sparkDataFrame
from pytz import timezone
import pytz


def find_time_points(
        start_date: str,
        period_list: List[int],
) -> Tuple[str]:
    """
    This function finds multiple time points based on date_period function.
    > find_time_points('20191201', [-1,0,1])
    > ['20191130', '20191201', '20191202']
    :param start_date: str and needs to be in YYYYMMDD format
    :param period_list: has to be a list of integers
    :return: Tuple of dates based on the order of the list input.
    """
    time_points = []
    for p in period_list:
        time_points.append(date_period(p, start_date)[1])

    return time_points


def find_start_date():
    # AWS's time zone is UTC.  Need to convert UTC to PST.
    today = datetime.now(tz = pytz.utc)
    today = today.astimezone(timezone('US/Pacific'))

    # If it's Monday, run with Friday data since transactions over the weekend are not updated yet.
    if today.weekday() == 0:  # Monday
        _, start_date = date_period(-3, datetime.strftime(today, '%Y%m%d'))
    elif today.weekday() == 6:  # Sunday
        _, start_date = date_period(-2, datetime.strftime(today, '%Y%m%d'))
    elif today.weekday() == 5:  # Saturday
        _, start_date = date_period(-1, datetime.strftime(today, '%Y%m%d'))
    else:
        start_date = datetime.strftime(today, '%Y%m%d')
    return start_date


def date_period(period: int, start_date: str = ''):
    """
    Author: Allison Wu
    Description: A function that returns end_date = start_date + period (in days).
    :param period: can be either positive or negative integers to specify the period
    :param start_date: has to be in YYYYMMDD format
    :return: end_date: in YYYYMMDD format
    """

    # TODO Check the stat_date format - Has to be YYYYMMDD
    if start_date == '':
        datetime_start_date = datetime.now()
    else:
        datetime_start_date = datetime.strptime(start_date, '%Y%m%d')

    end_date: str = datetime.strftime(datetime_start_date + timedelta(days=period), '%Y%m%d')
    start_date: str = datetime.strftime(datetime_start_date, '%Y%m%d')
    return start_date, end_date


def add_ltm_period(ih: sparkDataFrame,
                   date_col_name: str = 'invc_dt',
                   date_format: str = 'YYYYMMDD'):
    """
    Author: Rich Winkler
    Description: A function which accepts as input a Spark Dataframe with invoice history data. One of the columns
    should be named the same as date_col_name
    :param ih: A Spark Dataframe containing some type of invoice history with a date column as specified in
        date_col_name
    :param date_col_name: The name of the date column in the ih dataframe
    :return: ih, a Spark Dataframe with a new column named ltm_period containing:
        0 - Prior Prior LTM data
        1 - Prior LTM data
        2 - LTM data
    """
    if date_col_name not in ih.columns:
        raise DataValidityError(
            f'Date column {date_col_name} not present in the table, please check invoice input.')

    if date_format == 'DATE':
        date_col_name = f"CAST(date_format(CAST({date_col_name} AS DATE),'YYYYMMdd') AS int)"
    ltm_period_dates = list(reversed([date_period(-365 * i)[1] for i in range(1, 4)]))
    ltm_period_dates.append(date_period(-365)[0])

    case_expr = 'CASE '
    for i in range(len(ltm_period_dates) - 1):
        start_dt = ltm_period_dates[i]
        end_dt = ltm_period_dates[i + 1]
        case_expr += f'WHEN {date_col_name} >= {start_dt} AND {date_col_name} < {end_dt} THEN {i} '
    case_expr += 'END'

    ih = ih.withColumn('ltm_period', F.expr(case_expr))
    ih = ih.filter(ih.ltm_period.isNotNull())
    return ih


@timer
def bound_date_check(
        table_name: str,
        dt_col_name: str,
        start_date: str,
        end_date: str,
        env: str,
        date_format: str = 'DATE', #YYYYMMDD
        division: str = 'CCG',
):
    """
    Author: Allison Wu

    :param table_name: table name you'd like to check dates on, eg. ccgds.f_sls_invc_model_vw
    :param dt_col_name: the date column you're checking for, eg. invc_dt
    :param start_date: start date of the bound date. Bound is exclusive. Errors are thrown when min_date > start_date.
    :param end_date: end date of the bound date.  Bound is exclusive. Errors are thrown when max_date < end_date.
    :param env: envrionment of the table
    :param date_format: the date format of the dates, takes either 'DATE' or 'YYYYMMDD'
    :param division: enter either 'CCG' or 'LSG' to make sure it uses the correct credentials
    :return: check_max (TRUE or FALSE), check_min (TRUE or FALSE), max_date, min_date
    """
    test_query = ''
    if date_format == 'DATE':
        test_query = \
            f"SELECT TO_CHAR(CAST(MAX({dt_col_name}) AS DATE),'YYYYMMDD') AS max_date,  " \
            f"TO_CHAR(CAST(MIN({dt_col_name}) AS DATE),'YYYYMMDD') AS min_date " \
            f"FROM {table_name} "

    elif date_format == 'YYYYMMDD':
        test_query = \
            f"SELECT MAX({dt_col_name}) AS max_date,  " \
            f"MIN({dt_col_name}) AS min_date " \
            f"FROM {table_name} "

    if division == 'CCG':
        bound_date_df = redshift_ccg_read(test_query, env = env, schema = None, cache = False).toPandas()
    elif division == 'LSG':
        bound_date_df = redshift_cdw_read(test_query, env = env, schema = None, cache = False).toPandas()

    max_date = str(bound_date_df['max_date'][0])
    min_date = str(bound_date_df['min_date'][0])
    check_max = True
    check_min = True

    if max_date < end_date:
        check_max = False

    if min_date > start_date:
        check_min = False

    if not check_max:
        raise EndDateTooLateError(
            f'max_date = {max_date} while end_date ={end_date}. Max date < end_date. '
            f'Check the validity of {table_name}.'
        )
        exit()

    if not check_min:
        raise StartDateTooEarlyError(
            f'min_date = {min_date} while start_date ={start_date}. min date > Start date. '
            f'Check the validity of {table_name}.'
        )
        exit()

    if check_max and check_min:
        print('Data table is complete within date range.')

    return check_max, check_min, max_date, min_date

