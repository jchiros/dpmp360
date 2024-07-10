import os
import pytz
from pyspark.sql.types import StringType, FloatType, IntegerType, \
  BooleanType, LongType, DoubleType, DecimalType


TIMEZONE = pytz.timezone("Asia/Manila")
landing_zone = os.environ.get('LANDING_ZONE')
catalog = os.environ.get('CATALOG')
JOB_CONFIG_LOC = 'job_configs'

SPARK_DATATYPES = {
  "float": FloatType(),
  "numeric": IntegerType(),
  "int": IntegerType(),
  "bigint": LongType(),
  "double": DoubleType(),
  "decimal": DecimalType(),
  "date": StringType(),
  "timestamp": StringType(),
  "boolean": BooleanType(),
  "string": StringType()
}

SPARK_CONFIG = {
    "spark.databricks.delta.preview.enabled": True,
    "spark.databricks.delta.schema.autoMerge.enabled": True
}
