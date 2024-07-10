import IPython
from datetime import datetime
from utils.const import TIMEZONE


dbutils = IPython.get_ipython().user_ns['dbutils']

def get_task_param(task: str,
                   key: str,
                   default: str=""):
    return dbutils.jobs.taskValues.get(taskKey=task,
                                       key=key,
                                       default=default)


def set_task_param(key: str,
                   value: str):
    return dbutils.jobs.taskValues.set(key=key,
                                       value=value)


def skip_task(log: str= ""):
    return dbutils.notebook.exit(log)


def custom_log(message: str= ""):
    datetime_now = str(datetime.now(TIMEZONE))
    print(f"[INFO] {datetime_now} {message}")
