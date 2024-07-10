from pyspark.sql.functions import to_date, lit, \
    monotonically_increasing_id, substring, col, max
from pyspark.sql import DataFrame


def transform_skip_rows(dataframe: DataFrame,
                        n_rows: int=0) -> DataFrame:
    if n_rows != 0:
        skip_rows = [i for i in range(0, n_rows)]
        dataframe = dataframe.withColumn('index', monotonically_increasing_id())
        dataframe = dataframe \
            .filter(~(dataframe.index.isin(skip_rows))) \
            .drop('index')
    return dataframe


def transform_skip_footer(dataframe: DataFrame,
                          n_rows: int=0) -> DataFrame:
    if n_rows != 0:
        dataframe = dataframe.withColumn('index_footer', monotonically_increasing_id())
        max_index = dataframe.agg(max('index_footer')).collect()[0][0]
        skip_footer = [i for i in range(max_index, max_index-n_rows, -1)]
        dataframe = dataframe \
            .filter(~(dataframe.index_footer.isin(skip_footer))) \
            .drop('index_footer')
    return dataframe


def transform_where_clause(dataframe: DataFrame,
                           clause: str='') -> DataFrame:
    if clause:
        dataframe = dataframe.where(clause)
    return dataframe


def transform_add_existasof(dataframe: DataFrame,
                            business_date: str) -> DataFrame:
    return dataframe.withColumn('existasof', to_date(lit(business_date), 'y-M-d'))
  

def transform_headers(dataframe: DataFrame,
                      schema) -> DataFrame:
    alias_col = [
        substring(col('value'),
            schema.fields[i].metadata['substring'][0],
            schema.fields[i].metadata['substring'][1]).cast(schema.fields[i].dataType).alias(schema.names[i])
        for i in range(len(schema))
    ]
    return dataframe.select(*alias_col)


def transform_add_column(dataframe: DataFrame,
                         column: str,
                         value: str) -> DataFrame:
    return dataframe.withColumn(column, lit(value))
