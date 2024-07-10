import os
import re
import json
import fnmatch
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from utils.const import JOB_CONFIG_LOC, TIMEZONE


def is_weekend(snapshot: str) -> bool:
    strtoweekday = str_to_datetime(snapshot).weekday()
    if (strtoweekday > 4):
        return True


def is_holiday(spark: SparkSession,
               catalog: str,
               snapshot: str) -> bool:
    query = spark.sql(f"""SELECT COUNT(start_date)
                      FROM {catalog}.cleansed_reference.ref_holidays
                      WHERE start_date = '{snapshot}'""")
    holiday = int(query.first()[0])
    if holiday:
        return True


def str_to_datetime(datestr: str) -> date:
    date_dict = get_date_components(datestr)
    return date(int(date_dict["year"]), int(date_dict["month"]), int(date_dict["day"]))


def datetime_to_str(datelist: list) -> list:
    return [x.strftime("%Y-%m-%d") for x in datelist]


def multi_snapshots(start_date: str,
                    end_date: str) -> list:
    snapshots = []
    for x in range((str_to_datetime(end_date) - str_to_datetime(start_date)).days + 1):
        snapshots.append(str_to_datetime(start_date)+timedelta(days=x))
    return datetime_to_str(snapshots)


def single_snapshot(start_date: str) -> list:
    snapshots = []
    snapshots.append(str_to_datetime(start_date))
    return datetime_to_str(snapshots)


def get_prev_day_snapshot() -> str:
    yesterday_snapshot = [datetime.now(TIMEZONE) - timedelta(1)]
    return datetime_to_str(yesterday_snapshot)


def config_load_strategy(snapshots: list='') -> str:
    if len(snapshots) == 1:
        return 'single_incremental'
    if len(snapshots) >= 2:
        return 'backload_incremental'
    if not snapshots:
        return 'full_refresh'


def config_spark_snapshot(start_date: str,
                          end_date: str) -> str:
    if start_date and end_date:
        if start_date == end_date:
            return single_snapshot(start_date)
        else:
            return multi_snapshots(start_date, end_date)
    else:
        return get_prev_day_snapshot()


def get_date_components(date: str) -> dict:
   year = date.split('-')[0]
   month = date.split('-')[1]
   day = date.split('-')[2]
   return {
      "month": month,
      "day": day,
      "year": year
    }


def parse_job_conf() -> dict:
    jobs = {}
    for job in os.listdir(JOB_CONFIG_LOC):
        job_name = job.split('.')[0]
        f = open(os.path.join(JOB_CONFIG_LOC, job))
        data = json.load(f)
        jobs[job_name] = data
    return jobs


def locate_files(pattern: str,
                 src: str='.') -> list:
    rule = re.compile(fnmatch.translate(pattern), re.IGNORECASE)
    return [name for name in os.listdir(src) if rule.match(name)]
