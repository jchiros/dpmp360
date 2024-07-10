import json


def parse_dbt_steps():
  f = open('py_dbt/dbt_steps.json')
  return json.load(f)


def is_all_jobs_required(dbt_config: dict) -> bool:
    return dbt_config['is_all_required']


def dbt_seed_raw_selections(command: str,
                            jobs: list,
                            dbt_config: dict) -> str:
    dbt_config = dbt_config['raw']
    if not is_all_jobs_required(dbt_config):
        command = 'dbt seed --select'
        for job in jobs:
            temp = next((step for step in dbt_config['jobs'] if job == step), None)
            if temp:
                command += f' {temp}'
    return command


def dbt_work_selections(command: str,
                        jobs: list,
                        dbt_config: dict) -> str:
    dbt_config = dbt_config['work']
    if not is_all_jobs_required(dbt_config):
        command = 'dbt run --select'
        for job in jobs:
            temp = next((step for step in dbt_config['jobs'] if job == step), None)
            if temp:
                command += f' {temp}'
    return command


def dbt_cleansed_selections(command: str,
                            jobs: list,
                            dbt_config: dict) -> str:
    dbt_config = dbt_config['cleansed']
    if not is_all_jobs_required(dbt_config):
        command = 'dbt run --select'
        for job in jobs:
            temp = next((step for step in dbt_config['jobs'] if job == step), None)
            if temp:
                command += f' {temp}'
    return command


def dbt_curated_selections(command: str,
                           jobs: list,
                           dbt_config: dict) -> str:
    dbt_config = dbt_config['curated']
    if not is_all_jobs_required(dbt_config):
        command = 'dbt run --select'
        for job in jobs:
            temp = next((step for step in dbt_config['jobs'] if job == step), None)
            if temp:
                command += f' {temp}'
    return command


def dbt_test_selections(command: str,
                        jobs: str,
                        dbt_config: dict) -> str:
    dbt_config = dbt_config['work']
    if not is_all_jobs_required(dbt_config):
        command = 'dbt test --select'
        for job in jobs:
            temp = next((step for step in dbt_config['jobs'] if job == step), None)
            if temp:
                command += f' {temp}'
    return command


def create_models_selection(dbt_step: dict,
                            command: str,
                            jobs: str) -> str:
    dbt_config = parse_dbt_steps()['steps']
    if dbt_step == 'raw':
        return dbt_seed_raw_selections(command, jobs, dbt_config)
    elif dbt_step == 'work':
        return dbt_work_selections(command, jobs, dbt_config)
    elif dbt_step == 'cleansed':
        return dbt_cleansed_selections(command, jobs, dbt_config)
    elif dbt_step == 'curated':
        return dbt_curated_selections(command, jobs, dbt_config)
    elif dbt_step == 'test':
        return dbt_test_selections(command, jobs, dbt_config)
    else:
        return command
        print(f"This {dbt_step} step isn't supported else running all")