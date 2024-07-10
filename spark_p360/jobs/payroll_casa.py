import os
import linecache
import pandas as pd
from functools import reduce
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame
from utils.workflow import custom_log
from utils.const import landing_zone, catalog
from utils.spark import write_to_uc_table, create_spark_schema
from utils.univ import get_date_components, is_weekend, is_holiday, \
    locate_files
from utils.transformer import transform_add_existasof

def init_vars(source: dict,
              schema: dict,
              snapshot: str) -> dict:
    file_pattern = source.get('file_pattern', '')
    file_path = source.get('path', '')
    skip_rows = source.get('skip_rows', 0)
    skip_footer = source.get('skip_footer', 0)
    pandas_schema = get_pandas_schema(schema)
    date_components = get_date_components(snapshot)
    src = file_path.format(mm=date_components['month'],
                           dd=date_components['day'],
                           yyyy=date_components['year'])
    return {
        "src": src,
        "file_pattern": file_pattern,
        "skip_rows": skip_rows,
        "skip_footer": skip_footer,
        "pandas_schema": pandas_schema,
        "schema": schema,
        "snapshot": snapshot
      }


def get_pandas_schema(schema):
    col_names = list()
    col_positions = list()
    for field in schema.keys():
        if schema[field].get('substring', ''):
            col_names.append(field)
            col_positions.append(tuple(schema[field]['substring']))
    return {
        "col_names": col_names,
        "col_positions": col_positions
    }


def extract_pandas_handoff(file: str,
                           schema: str,
                           skip_rows: int,
                           skip_footer: int):
    pandas_df = pd.read_fwf(filepath_or_buffer=file,
                            header=None,
                            dtype=str,
                            skiprows=skip_rows,
                            skipfooter=skip_footer,
                            colspecs=schema['col_positions'],
                            names=schema['col_names'])
    
    extras = extract_other_data(file)
    for col, val in extras.items():
        pandas_df[col] = val
    return pandas_df


def extract_other_data(file: str,
                       rows: int=3):
    extras = {}
    for row in range(1, rows):
        data = linecache.getline(file, row)
        if row == 1:
            extras['branch'] = data[85:89]
            extras['company_code'] = data[89:92]
            extras['handoff_name'] = data[23:47].strip()
        elif row == 2:
            extras['company_account'] = data[80:96]
        else:
            continue
    return extras


def align_schema(dataframe: DataFrame,
                 schema: dict,
                 partition_col: str='existasof') -> DataFrame:
    columns = [col for col in schema.keys()]
    columns.append(partition_col)
    return dataframe.select(columns)


def validator_src(src_files: list):
    if not src_files:
        raise Exception(f"No handoff files found!")


def is_file_valid(file: str) -> bool:
    return 'liabilities' not in file.lower() and \
           '.' in file.lower() and \
           os.stat(file).st_size > 0


def collate_dfs(spark: SparkSession,
                params: dict) -> DataFrame:
    pandas_dfs = list()
    dirty_files = list()
    src = f"{landing_zone}/{params['src']}"
    src_files = locate_files(pattern=params['file_pattern'],
                             src=f"{landing_zone}/{params['src']}")
    validator_src(src_files)
    for file in src_files:
        custom_log(f"Extracting {file}")
        src_file = f"{src}/{file}"
        if is_file_valid(file=src_file):
            pandas_df = extract_pandas_handoff(file=src_file,
                                               schema=params['pandas_schema'],
                                               skip_rows=params['skip_rows'],
                                               skip_footer=params['skip_footer'])
            pandas_dfs.append(pandas_df)
        else:
            dirty_files.append(file)
            custom_log(f"This {file} file has been discarded.")
    custom_log(f"Discarded files ({str(len(dirty_files))}): {', '.join(dirty_files)}")
    spark_df = spark.createDataFrame(pd.concat(pandas_dfs))
    spark_df = transform_add_existasof(spark_df, params['snapshot'])
    spark_df = align_schema(dataframe=spark_df,
                             schema=params['schema'])
    return spark_df


def extract_payroll_casa(spark: SparkSession,
                         source: dict,
                         destination: dict,
                         schema: dict,
                         snapshot: str,
                         table: str='payroll_casa'):
    params = init_vars(source=source,
                       schema=schema,
                       snapshot=snapshot)
    spark_df = collate_dfs(spark=spark,
                           params=params)
    spark_df.createOrReplaceTempView(table)
    res = write_to_uc_table(spark=spark,
                            destination=destination,
                            snapshot=params['snapshot'])


def run(spark: SparkSession,
        source: dict,
        destination: dict,
        schema: dict,
        settings: dict,
        snapshot: str):
    return extract_payroll_casa(spark=spark,
                                source=source,
                                destination=destination,
                                schema=schema,
                                snapshot=snapshot)
