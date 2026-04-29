# Databricks notebook source
# MAGIC %md
# MAGIC **Python: Creating a Kinesis Data Stream**
# MAGIC
# MAGIC **Description:**
# MAGIC
# MAGIC The below Python code demonstrates how to create a Kinesis Data Stream using the Boto3 library. It imports the `boto3` module, sets the desired stream name and region, creates a client object, and calls the `create_stream()` method with the specified parameters.

# COMMAND ----------

# Run shared config notebook to load AWS/environment helper functions and Spark conf values.
# MAGIC %run ../_resources/01-config

# COMMAND ----------

# Build the cloud storage base path from configured workshop bucket.
cloud_storage_path = 's3://' + spark.conf.get("da.workshop_bucket")

# COMMAND ----------

# Import AWS SDK used to manage and write to Kinesis.
import boto3

# Define the Kinesis stream name used by the ingestion lab.
kinesisStreamName = "StockTickerStream"
# Resolve current AWS region from metadata helper.
kinesisRegion = get_region()
# Create a Kinesis client scoped to the resolved region.
client = boto3.client('kinesis', region_name=kinesisRegion)
# Attempt to detect an existing stream before creating a new one.
try:
    # Fetch stream metadata by name.
    response = client.describe_stream(StreamName=kinesisStreamName)
    # Check whether the stream already exists and is active.
    if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
        # Inform the user that stream creation is not required.
        print(f"Stream '{kinesisStreamName}' already exists.")
        #return
except client.exceptions.ResourceNotFoundException:
    # Inform the user that stream creation will begin.
    print(f"Stream '{kinesisStreamName}' does not exist. Creating...")
    # Create a single-shard Kinesis stream for workshop data generation.
    response = client.create_stream(
        StreamName=kinesisStreamName,
        ShardCount=1  # Specify the desired number of shards
    )
    # Confirm successful stream creation.
    print(f"Stream '{kinesisStreamName}' created successfully.")

# response = client.create_stream(
#     StreamName=kinesisStreamName,
#     StreamModeDetails={
#         'StreamMode': 'ON_DEMAND'
#     }
# )

# COMMAND ----------

# Disable listShards optimization setting for compatibility in this lab setup.
spark.conf.set("spark.databricks.kinesis.listShards.enabled", False)

# COMMAND ----------

# Import datetime helper for event timestamps.
import datetime
# Import JSON utilities for serializing Kinesis payloads.
import json
# Import random utilities to generate sample ticker and price values.
import random

# Define helper to generate one synthetic stock ticker event payload.
def get_data():
    # Return one event record with current timestamp and randomized values.
    return {
        'event_time': datetime.datetime.now().isoformat(),
        'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
        'price': round(random.random() * 100, 2)}


# Define helper to publish a batch of synthetic events to Kinesis.
def generate(stream_name, kinesis_client):
    # Send 300 records to simulate an input stream workload.
    for val in range(300):
        # Build one event payload.
        data = get_data()
        #print(data)
        # Publish serialized event to the target Kinesis stream.
        kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey="partitionkey")

# COMMAND ----------


