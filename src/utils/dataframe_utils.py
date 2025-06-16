# src/utils/dataframe_utils.py
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("Health Insurance Data pipeline").getOrCreate()

def read_data_spark(file_path: str, file_format: str, **options):
    """
    Read data via Spark.  
    file_format is e.g. 'csv', 'parquet', etc.
    """
    return spark.read.format(file_format).options(**options).load(file_path)

def write_data_spark(
    file_path: str,
    file_format: str,
    df,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
    **options
):
    """
    Write a Spark DataFrame out.
    """
    writer = df.write.format(file_format).mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.options(**options).save(file_path)

