import sys
import ast
from py_dbt.workflow import get_task_param, custom_log
from py_dbt.executor import run_dbt as run


parameters = sys.argv[-1]

parameters = ast.literal_eval(parameters)
dbt_step = parameters['dbt_step']
dbt_command = parameters['dbt_command']
dbt_steps = get_task_param("init_config", key="dbt_steps")
jobs = get_task_param("init_config", key="jobs_to_run")
snapshot = get_task_param("init_config", key="snapshot")
start_date = get_task_param("init_config", key="start_date")
end_date = get_task_param("init_config", key="end_date")
load_strategy = get_task_param("init_config", key="load_strategy")
partition_column = "existasof"


if __name__ == "__main__":

    if dbt_step in dbt_steps:
        custom_log(f"""Fetch parameters: 
              Job: {jobs} 
              Load strategy: {load_strategy}
              Snapshots: {snapshot}
              Partition column: {partition_column}
              Bare dbt command: {dbt_command}""")
        run(command=dbt_command,
            jobs=jobs,
            dbt_step=dbt_step,
            partition_column=partition_column,
            load_strategy=load_strategy,
            snapshot=snapshot,
            start_date=start_date,
            end_date=end_date)
    else:
        custom_log(f"This {dbt_step} step was skip")

