import sys
from utils.const import SPARK_CONFIG
from utils.workflow import get_task_param, custom_log, skip_task
from utils.spark import create_spark_session, create_uc_table, \
    read_write_spark_on_job
from jobs.payroll_casa import run as payroll_casa
from jobs.payroll_icard import run as payroll_icard


# get current task parameter
job = sys.argv[-1]

# get the previous step task parameter
snapshots = get_task_param(task="init_config", key="snapshot")
jobs = get_task_param(task="init_config", key="jobs")
jobs_to_run = get_task_param(task="init_config", key="jobs_to_run")

# create spark session
# add necessary configuration on constant.py
spark = create_spark_session(SPARK_CONFIG, app_name="spark_p360")

# ingestion job
def run():
    if job in jobs_to_run or jobs_to_run == 'all':
        if job in jobs.keys():
          job_config = jobs[job]
          source = job_config['source']
          destination = job_config['destination']
          schema = job_config['schema']
          settings = job_config.get('settings', {})

          custom_log(f"A {job} job has started")
          create_uc_table(spark=spark, destination=destination, schema=schema)
          job_func = getattr(sys.modules[__name__], source['script'])
          read_write_spark_on_job(spark=spark, job_func=job_func, 
                                  settings=settings, destination=destination,
                                  schema=schema, snapshots=snapshots, source=source)
        else:
            custom_log(f"A {job} job is not implemented")
            sys.exit(1)
    else:
        skip_task(log=f"A {job} job was skip since it's not part of the jobs_to_run param")


# run the ingestion job
if __name__ == "__main__":
    run()
