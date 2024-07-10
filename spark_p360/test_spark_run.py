import sys
from utils.const import SPARK_CONFIG
from utils.workflow import custom_log, skip_task
from utils.spark import create_spark_session, create_uc_table, \
    read_write_spark_on_job
from jobs.payroll_icard import run as payroll_icard
from jobs.payroll_casa import run as payroll_casa

# get current task parameter
job = 'payroll_icard'


# get the previous step task parameter
snapshots = ['2024-01-05']
jobs = {
    'payroll_casa': {
        "schema": {
            "rec_type": {
                "datatype": "string",
                "nullable": True,
                "substring": [0, 1]
            },
            "batch_id": {
                "datatype": "string",
                "nullable": True,
                "substring": [1, 12]
            },
            "batch_date": {
                "datatype": "string",
                "nullable": True,
                "substring": [13, 21]
            },
            "serial_number": {
                "datatype": "string",
                "nullable": True,
                "substring": [21, 26]
            },
            "category_of_interface": {
                "datatype": "string",
                "nullable": True,
                "substring": [26, 28]
            },
            "currency": {
                "datatype": "string",
                "nullable": True,
                "substring": [28, 31]
            },
            "tran_type": {
                "datatype": "string",
                "nullable": True,
                "substring": [31, 32]
            },
            "tran_sub_type": {
                "datatype": "string",
                "nullable": True,
                "substring": [32, 34]
            },
            "tran_date": {
                "datatype": "string",
                "nullable": True,
                "substring": [34, 42]
            },
            "value_date": {
                "datatype": "string",
                "nullable": True,
                "substring": [42, 50]
            },
            "initiating_sol": {
                "datatype": "string",
                "nullable": True,
                "substring": [50, 55]
            },
            "gi_date": {
                "datatype": "string",
                "nullable": True,
                "substring": [55, 63]
            },
            "tran_amount": {
                "datatype": "string",
                "nullable": True,
                "substring": [63, 80]
            },
            "act_no": {
                "datatype": "string",
                "nullable": True,
                "substring": [80, 96]
            },
            "tran_particular": {
                "datatype": "string",
                "nullable": True,
                "substring": [96, 296]
            },
            "part_tran_type": {
                "datatype": "string",
                "nullable": True,
                "substring": [296, 297]
            },
            "office_account_placeholder": {
                "datatype": "string",
                "nullable": True,
                "substring": [297, 313]
            },
            "bank_id": {
                "datatype": "string",
                "nullable": True,
                "substring": [313, 315]
            },
            "company_account": {
                "datatype": "string",
                "nullable": True
            },
            "branch": {
                "datatype": "string",
                "nullable": True
            },
            "company_code": {
                "datatype": "string",
                "nullable": True
            },
            "handoff_name": {
                "datatype": "string",
                "nullable": False
            }
        },
        "source": {
            "script": "payroll_casa",
            "path": "RCBC/PAYROLL/DAILY/{mm}{dd}{yyyy}",
            "file_pattern": "*.t**",
            "skip_rows": 2,
            "skip_footer": 1
        },
        "destination": {
            "schema": "raw_rcbc",
            "table": "payroll_casa",
            "load_strategy": "incremental",
            "partition_col": "existasof",
            "partition_dtype": "date"
            },
        "settings":{
            "weekends": {
                "load": False,
                "replace_by": "latest_snapshot"
            },
            "holidays": {
                "load": False,
                "replace_by": "latest_snapshot"
            }
        }
    },
    'payroll_icard': {
        "schema": {
            "rec_type": {
                "datatype": "string",
                "nullable": True,
                "substring": [0, 1]
            },
            "act_no": {
                "datatype": "string",
                "nullable": True,
                "substring": [1, 17]
            },
            "tran_amount": {
                "datatype": "string",
                "nullable": True,
                "substring": [17, 32]
            },
            "payee": {
                "datatype": "string",
                "nullable": True,
                "substring": [32, 132]
            },
            "company_account": {
                "datatype": "string",
                "nullable": True
            },
            "co_name": {
                "datatype": "string",
                "nullable": True
            },
            "tran_date": {
                "datatype": "string",
                "nullable": True
            },
            "tran_date_orig": {
                "datatype": "string",
                "nullable": True
            }
        },
        "source": {
            "script": "payroll_icard",
            "path": "/RCBC/PAYROLL/DAILY/{mm}{dd}{yyyy}",
            "file_pattern": "*.i****",
            "skip_rows": 2,
            "skip_footer": 1
        },
        "destination": {
            "schema": "raw_rcbc",
            "table": "payroll_icard",
            "load_strategy": "incremental",
            "partition_col": "existasof",
            "partition_dtype": "date"
        },
        "settings": {
            "weekends": {
                "load": False,
                "replace_by": "latest_snapshot"
            },
            "holidays": {
                "load": False,
                "replace_by": "latest_snapshot"
            }
        }
    }
}
jobs_to_run = ['payroll_casa', 'payroll_icard']


# create spark session
# add necessary configuration on constant.py
spark = create_spark_session(SPARK_CONFIG, app_name="spark_payroll")


# ingestion job
def run():
    if job in jobs_to_run or jobs_to_run == 'all':
        if job in jobs.keys():
          job_config = jobs[job]
          
          source = job_config['source']
          destination = job_config['destination']
          schema = job_config['schema']
          settings = job_config['settings']

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
