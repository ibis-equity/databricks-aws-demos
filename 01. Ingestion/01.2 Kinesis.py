# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2: Consuming Kinesis Streams

# COMMAND ----------

# MAGIC %run ../_resources/01-setup

# Databricks `%run` injects these names at runtime; alias them for IDE/static analysis.
# Import typing support used by the fallback helper function signature.
from typing import Any

# Resolve the data generator function injected by the setup notebook.
generate = globals().get("generate")
# Resolve the Kinesis stream name injected by the setup notebook.
kinesisStreamName = globals().get("kinesisStreamName")
# Resolve the AWS region for the Kinesis stream.
kinesisRegion = globals().get("kinesisRegion")
# Resolve the boto3 client injected by setup for AWS API operations.
client = globals().get("client")
# Resolve cloud storage path used for streaming checkpoints.
cloud_storage_path = globals().get("cloud_storage_path")
# Resolve SparkSession injected by Databricks runtime.
spark = globals().get("spark")

# Guard against missing setup execution by providing a clear fallback.
if generate is None:
    # Provide a stub that fails fast when the setup notebook was not executed.
    def generate(*_args: Any, **_kwargs: Any) -> Any:  # type: ignore[no-redef]
        # Raise a descriptive runtime error to guide notebook users.
        raise RuntimeError("`generate` is expected from `%run ../_resources/01-setup`.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using AWS SDK for Python (Boto3) to create resources
# MAGIC The approach is used in a lab environment. In a normal production environment these cloud resources would be created by the infrastructure team.
# MAGIC <a>https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html</a>

# COMMAND ----------

# Push sample events into Kinesis so downstream streaming reads have input data.
generate(kinesisStreamName, client)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  2. Reading from Kinesis Streams
# MAGIC We use Spark Structured Streaming to read data from a Kinesis Stream. Let's define the necessary parameters and read the data.

# COMMAND ----------

# Create a structured streaming DataFrame that reads records from Kinesis.
kinesisData = (spark.readStream
                  .format("kinesis")
                  .option("streamName", kinesisStreamName)
                  .option("region", kinesisRegion)
                  .option("initialPosition", 'TRIM_HORIZON')
                  .load()
                )

# COMMAND ----------

# DBTITLE 1,Define a Schema to use
# Import PySpark SQL types used to define the JSON payload schema.
from pyspark.sql.types import *

# Define the expected JSON schema for stock ticker events.
pythonSchema = StructType() \
          .add("event_time", TimestampType()) \
          .add("ticker", StringType()) \
          .add ("price", DoubleType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Serialization/Deserialization
# MAGIC Kinesis data is binary, we need to convert the data into a usable format. Let's assume the data is in UTF-8 encoded strings.
# MAGIC
# MAGIC ```
# MAGIC from pyspark.sql.functions import col
# MAGIC kinesisDF = kinesisDF.selectExpr("CAST(data AS STRING)")
# MAGIC ```

# COMMAND ----------

# Import JSON parsing helper to deserialize string payloads into structured columns.
from pyspark.sql.functions import from_json

# Cast binary Kinesis data to string and parse it into typed columns.
kinesisDF = kinesisData.selectExpr("cast (data as STRING) jsonData") \
            .select(from_json("jsonData", pythonSchema).alias("payload")) \
            .select("payload.*")
#display(kinesisDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  4. Sink Data to a Delta Table
# MAGIC Delta Lake provides several advantages over regular Parquet. It provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing.
# MAGIC
# MAGIC Let's sink the data from the stream to a Delta table.

# COMMAND ----------

# Write the parsed stream to a Delta table with a persistent checkpoint location.
kinesisDF.writeStream \
  .format("delta") \
  .option("checkpointLocation", cloud_storage_path + "/delta/checkpoints") \
  .table("stock_ticker")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM stock_ticker

# COMMAND ----------

# Add another batch of records to demonstrate incremental streaming ingestion.
generate(kinesisStreamName, client)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM stock_ticker

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 5. Error Handling and Recovery
# MAGIC Structured Streaming can recover from failures and continue processing where it left off. We'll have to specify a checkpoint location in case of failure. In the above example, /delta/checkpoints is the checkpoint location.
# MAGIC
# MAGIC ### 6. Checkpoints and Job Restarts
# MAGIC Checkpoints store the current state of a streaming query, which can be used to restart the query in case of a failure. You can monitor and analyze checkpoints using Databricks’ built-in structured streaming sink, or by inspecting the checkpoint files directly in the file system.
# MAGIC
# MAGIC To restart a failed job from a checkpoint, simply start the query with the same checkpoint location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Watermarking and Window Aggregations
# MAGIC If your stream processing includes handling late data or performing window-based operations, you can specify a watermark delay threshold and use window functions for aggregations.
# MAGIC
# MAGIC ```
# MAGIC kinesisDF.withWatermark("timestamp", "10 minutes") \
# MAGIC   .groupBy(window(kinesisDF.timestamp, "10 minutes")) \
# MAGIC   .count()
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Performance Monitoring and Tuning
# MAGIC You can use AWS CloudWatch and Databricks' built-in Spark UI to monitor the performance of the Kinesis ingestion job. Adjust the number of shards in Kinesis and parameters such as maxRecordsPerFetch and maxRatePerShard in Databricks based on your monitoring insights to achieve optimal performance.
# MAGIC
# MAGIC ### 9. Security and Access Control
# MAGIC IAM roles are used to securely access Kinesis Streams from Databricks. Make sure to assign necessary permissions to your IAM role to read from Kinesis and write to S3 (for checkpointing and sinking data to Delta).
# MAGIC
# MAGIC ### 10. Best Practices
# MAGIC Follow these tips for efficiently consuming Kinesis streams with Databricks:
# MAGIC
# MAGIC - Regularly monitor your jobs and tune your Kinesis shards and Databricks parameters for best performance.
# MAGIC - Use a secure IAM role with minimal necessary permissions.
# MAGIC - If you are dealing with late data or require window-based operations, use watermarking and window functions.
# MAGIC - Make use of Delta Lake's features such as ACID transactions and unified batch and streaming processing.

# COMMAND ----------

# DBTITLE 1,Clean up resources
# Delete the Kinesis stream at the end of the lab to avoid leftover resources.
response = client.delete_stream(
    StreamName=kinesisStreamName
)

# COMMAND ----------








