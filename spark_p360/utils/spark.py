import os
from utils.const import SPARK_DATATYPES
from utils.workflow import custom_log
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def create_spark_session(spark_config: dict,
                         app_name: str='spark_atm'):
    spark_builder = SparkSession.builder.appName(app_name)

    for k, v in spark_config.items():
        spark_builder.config(k, v)

    spark_session = spark_builder \
        .config('spark.sql.hive.metastore.jars', '/databricks/databricks-hive/*') \
        .enableHiveSupport() \
        .getOrCreate()
    return spark_session


def create_spark_schema(schema: dict):
    fields = []
    for field, value in schema.items():
        substring = value.get('substring', [])
        if substring:
            default = StringType()
            datatype = value['datatype']
            nullable = value['nullable']
            spark_datatype = SPARK_DATATYPES.get(datatype, default)
            fields.append(StructField(field,
                                      spark_datatype,
                                      nullable,
                                      metadata={'substring': substring}))
    schema = StructType(fields)
    return schema


def _fields_create_uc_table(schema: dict):
    fields = []
    for field, element in schema.items():
        fields.append(f"{field} {element['datatype']}")
    return ', '.join(fields)


def _uc_table_exists(spark: SparkSession,
                     destination: dict):
    uc_catalog = os.environ.get('CATALOG')
    uc_schema = destination['schema']
    uc_table = destination['table']
    query = f"""SELECT 1 
            FROM {uc_catalog}.information_schema.tables 
            WHERE table_name = '{uc_table}' 
            AND table_schema='{uc_schema}' LIMIT 1"""

    return spark.sql(query).count() > 0


def create_uc_table(spark: SparkSession,
                    destination: dict,
                    schema: dict):
    uc_catalog = os.environ.get('CATALOG')
    uc_schema = destination['schema']
    uc_table = destination['table']
    location = os.environ.get('STORAGE')
    partition_col = destination['partition_col']
    partition_dtype = destination['partition_dtype']
    uc_hierarchy_namespaces = f"{uc_catalog}.{uc_schema}.{uc_table}"
    
    query = f"""CREATE TABLE IF NOT EXISTS 
            {uc_hierarchy_namespaces} ({_fields_create_uc_table(schema)})
            USING DELTA
            PARTITIONED BY ({partition_col} {partition_dtype})
            LOCATION '{location}/{uc_schema}/{uc_table}'"""
    custom_log(query)
    if not _uc_table_exists(spark, destination):
        spark.sql(query)
        custom_log(f"""A {uc_hierarchy_namespaces} table has been created""")


def truncate_uc_table(spark: SparkSession,
                      destination: dict):
    uc_catalog = os.environ.get('CATALOG')
    uc_schema = destination['schema']
    uc_table = destination['table']
    try:
        query = f"TRUNCATE TABLE {uc_catalog}.{uc_schema}.{uc_table}"
        spark.sql(query)
    except Exception as e:
        raise(f"Error found -- {e}")


def write_to_uc_table(spark: SparkSession,
                      destination: dict,
                      snapshot: str):
    uc_catalog = os.environ.get('CATALOG')
    uc_schema = destination['schema']
    uc_table = destination['table']
    partition_col = destination['partition_col']
    query = f"""INSERT INTO {uc_catalog}.{uc_schema}.{uc_table} 
            REPLACE WHERE {partition_col} = '{snapshot}'
            SELECT * FROM {uc_table}"""
    spark.sql(query)
    custom_log(f"""A {uc_table} table 
                with snapshot {snapshot} 
                has been loaded to {uc_catalog}.{uc_schema}.{uc_table}""")


def incremental(spark: SparkSession,
                job_func,
                schema: dict,
                snapshots: list,
                source: dict,
                destination: dict, 
                settings: dict):
    for snapshot in snapshots:
        job_func(spark=spark,
                 schema=schema,
                 source=source,
                 snapshot=snapshot,
                 destination=destination,
                 settings=settings)


def read_write_spark_on_job(spark: SparkSession,
                            job_func,
                            settings: dict,
                            destination: dict,
                            schema: dict,
                            snapshots: list,
                            source: dict):
    spark_load_strategy = destination['load_strategy']
    if spark_load_strategy == 'incremental':
        incremental(spark=spark,
                    job_func=job_func,
                    schema=schema,
                    settings=settings,
                    snapshots=snapshots,
                    source=source,
                    destination=destination)
    else:
        raise('Strategy is not implemented!')
