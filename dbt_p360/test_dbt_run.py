import ast
from py_dbt.workflow import get_task_param, custom_log
from py_dbt.executor import run_dbt as run


parameters = {'dbt_step': 'cleansed', 'dbt_command': 'dbt run --select models/cleansed'}
# parameters = {'dbt_step': 'cleansed', 'dbt_command': 'dbt run --select models/cleansed'}


dbt_step = parameters['dbt_step']
dbt_command = parameters['dbt_command']
dbt_steps = ['work', 'test', 'deps', 'cleansed', 'curated']
jobs = ["payroll_icard", "payroll_casa", "stg_payroll_casa", "stg_payroll_icard"]
snapshot = ['2024-01-01']
start_date = '2024-01-01'
end_date = '2024-01-01'
load_strategy = 'single_incremental'
partition_column = 'existasof'



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
        custom_log(f"A {dbt_step} step was skip")
