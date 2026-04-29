# Databricks notebook source
# Databricks notebook source
# Import Delta Live Tables API used to declare managed pipeline tables.
import dlt
# Import PySpark SQL functions used in CDC transformations and expressions.
from pyspark.sql.functions import *
# Import PySpark SQL types used for explicit column typing.
from pyspark.sql.types import *

# Read the base input path for CDC files from pipeline Spark configuration.
data = spark.conf.get('data')
# Define source table metadata, including primary-key columns for merge logic.
tables = {
  'customers': {'id':'customerNumber'}
}

# Define a helper that declares DLT raw/target tables and CDC merge behavior per source table.
def generate_tables(table, info):
  # Declare a temporary bronze streaming table that ingests raw DMS CDC files.
  @dlt.table(
    name=f"{table}_cdc_raw",
    table_properties={ "quality": "bronze"},
    comment=f"Raw MySQL Data from DMS for the table: {table}",
    temporary=True
  )
  # Build the raw streaming DataFrame for one source table.
  def create_call_table():
    # Configure Auto Loader to continuously ingest CSV CDC files from cloud storage.
    stream = spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format", "csv")\
              .option("cloudFiles.inferSchema", "true")\
              .option("cloudFiles.inferColumnTypes", "true")\
              .load(f"{data}/{table}")
    
    # Ensure delete-operation column exists even if absent from some incoming files.
    if 'Op' not in stream.columns:
      # Add missing operation column as nullable string to keep schema consistent.
      stream = stream.withColumn("Op", lit(None).cast(StringType()))
    
    # Attach source file name metadata to each ingested record.
    return stream.withColumn("_ingest_file_name", input_file_name())
  
  # Declare the streaming silver target table that will receive merged CDC state.
  dlt.create_streaming_live_table(
    name=f"{table}",
    comment="Silver(Merged) MySQL Data from DMS for the table: {table}"
    )

  # Apply CDC merge semantics from raw stream into the target silver table.
  dlt.apply_changes(
    target = f"{table}",
    source = f"{table}_cdc_raw",
    keys = [info['id']],
    sequence_by = col("dmsTimestamp"),
    apply_as_deletes = expr("Op = 'D'"),
    except_column_list = ["Op", "dmsTimestamp", "_rescued_data"],
    stored_as_scd_type = 1
  )
    
# Generate DLT table definitions for each configured source table.
for table,info in tables.items():
  # Invoke table generation helper using each table's metadata configuration.
  generate_tables(table,info)
