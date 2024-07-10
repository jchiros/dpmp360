import subprocess
from py_dbt.workflow import custom_log
from py_dbt.utils import create_dbt_vars
from py_dbt.model_selections import create_models_selection


# Define subprocess to run any cli commands
def run_cli(command: str,
            dbt_vars: str):
    command = command.split(' ')
    if dbt_vars:
        command.append('--vars')
        command.append(dbt_vars)
    custom_log(f"Running {' '.join(command)}")
    process = subprocess.Popen(command,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    while True:
        output = process.stdout.readline()
        if output == b'' and process.poll() is not None:
            break
        if output:
           custom_log(output.decode().strip())

    if process.returncode != 0:
        raise Exception("An error occured. Please validate your dbt module.")


# Initializer to set everything that dbt needs
def init_run(command: str,
             load_strategy: str,
             jobs: list,
             dbt_step: str,
             start_date: str='',
             end_date: str='',
             snapshot: str='',
             partition_column: str='') -> dict:
    dbt_vars = create_dbt_vars(partition_column=partition_column,
                               load_strategy=load_strategy,
                               start_date=start_date,
                               end_date=end_date,
                               snapshot=snapshot)
    if jobs not in ['all', 'custom']:
        command = create_models_selection(dbt_step=dbt_step,
                                          command=command,
                                          jobs=jobs)
    elif jobs == 'custom':
        pass
    
    return {
        "dbt_vars": dbt_vars,
        "dbt_command": command
    }


# Execute dbt through subprocess
def run_dbt(command: str,
            load_strategy: str,
            jobs: list,
            dbt_step: str,
            start_date: str='',
            end_date: str='',
            snapshot: str='',
            partition_column: str=''):
    res = init_run(command, load_strategy, jobs, dbt_step, 
                   start_date, end_date, snapshot, partition_column)
    run_cli(res['dbt_command'], res['dbt_vars'])
