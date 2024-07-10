import subprocess
from py_dbt.workflow import custom_log


def multi_inc_predicates(partition_col: str,
                         start_date: str,
                         end_date: str) -> str:
    return f">= date'{start_date}' AND {partition_col} <= date'{end_date}'"


def single_inc_predicates(snapshot: list) -> str:
    return f"= date'{snapshot[0]}'"


def create_inc_predicates(partition_column: str,
                          load_strategy: str, 
                          start_date: str,
                          end_date: str, 
                          snapshot: list) -> dict:
    inc_predicates = ''
    if load_strategy == 'backload_incremental':
        inc_predicates = multi_inc_predicates(partition_col=partition_column,
                                              start_date=start_date,
                                              end_date=end_date)
    elif load_strategy == 'single_incremental':
        inc_predicates = single_inc_predicates(snapshot=snapshot)
    else:
        raise Exception("Specify loading strategy!")
    return {
        'incremental_predicates': inc_predicates
    }


def create_optional_vars(start_date: str,
                         end_date: str,
                         snapshot: list) -> dict:
    if not start_date and not end_date:
        start_date = snapshot[0]
        end_date = snapshot[0]
    return {
        'start_date': f"date'{start_date}'",
        'end_date': f"date'{end_date}'"
    }


def create_dbt_vars(partition_column: str,
                    load_strategy: str, 
                    start_date: str,
                    end_date: str,
                    snapshot: list) -> str:
    dbt_vars = ''
    if 'incremental' in load_strategy:
        inc_predicates = create_inc_predicates(partition_column=partition_column,
                                               load_strategy=load_strategy,
                                               start_date=start_date,
                                               end_date=end_date,
                                               snapshot=snapshot)
        optional_vars = create_optional_vars(start_date=start_date,
                                             end_date=end_date,
                                             snapshot=snapshot)
        dbt_vars = str({**inc_predicates, **optional_vars},)
    elif 'full_refresh' in load_strategy:
        custom_log(f'This loading strategy {load_strategy} is not allowed on this table.')
    return dbt_vars