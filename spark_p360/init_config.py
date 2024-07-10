import ast
import sys
from utils.workflow import set_task_param, custom_log
from utils.univ import parse_job_conf, config_spark_snapshot, config_load_strategy


jobs = parse_job_conf()

raw_parameters = sys.argv[-1]
parameters = ast.literal_eval(raw_parameters)

start_date = parameters['start_date']
end_date = parameters['end_date']
jobs_to_run = parameters['jobs']
dbt_steps = parameters['dbt_steps']

snapshots = config_spark_snapshot(start_date, end_date)
load_strategy = config_load_strategy(snapshots)


# set task values
custom_log(f"""Set init_config task values to the ff:
            Snapshots to process: {snapshots}
            Jobs: {jobs_to_run}
            Loading strategy: {load_strategy}
            DBT Steps: {dbt_steps}""")

set_task_param(key='start_date', value=start_date)
set_task_param(key='end_date', value=end_date)
set_task_param(key='snapshot', value=snapshots)
set_task_param(key='jobs_to_run', value=jobs_to_run)
set_task_param(key='jobs', value=jobs)
set_task_param(key='load_strategy', value=load_strategy)
set_task_param(key='dbt_steps', value=dbt_steps)
