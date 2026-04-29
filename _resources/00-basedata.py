# Databricks notebook source
# Create a widget to control whether demo data should be reset.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# Initialize the ingest file counter for standard ingestion path.
file_counter = 1
# Initialize the ingest file counter for SNS ingestion path.
file_counter_sns = 1
# Convert the reset widget value to a boolean flag.
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# Clear existing ingest folders when reset is requested.
if reset_all_data:
    # Remove all files from the standard ingest folder.
    dbutils.fs.rm(cloud_storage_path + "/ingest/", True)
    # Remove all files from the SNS ingest folder.
    dbutils.fs.rm(cloud_storage_path + "/ingest_sns/", True)

# COMMAND ----------

# Copy a reference CSV file into cloud storage for demo use.
dbutils.fs.cp(f"/databricks-datasets/flights/departuredelays.csv", cloud_storage_path + f"/csv/departuredelays.csv")

# COMMAND ----------

# Define a helper to copy one IoT file into the standard ingest folder.
def move_file(x):
    # Copy one source file into the ingest location using the current counter.
    dbutils.fs.cp(f"/databricks-datasets/iot-stream/data-device/part-{x:05}.json.gz", cloud_storage_path + f"/ingest/part-{x:05}.json.gz")
    # Increment the counter after copying one file.
    x = x + 1
    # Return the updated counter value.
    return x

# COMMAND ----------

# Define a helper to copy one IoT file into the SNS ingest folder.
def move_file_sns(x):
    # Copy one source file into the SNS ingest location using the current counter.
    dbutils.fs.cp(f"/databricks-datasets/iot-stream/data-device/part-{x:05}.json.gz", cloud_storage_path + f"/ingest_sns/part-{x:05}.json.gz")
    # Increment the counter after copying one file.
    x = x + 1
    # Return the updated counter value.
    return x

# COMMAND ----------

# Define a helper that adds a small batch of files to the ingest folder.
def add_data(x):
    # Start counting from the incoming counter value.
    count = x
    # Copy three files to simulate newly arriving data.
    for val in range(3):
        # Copy one file and update the running counter.
        count = move_file(count)
    # Return the counter after batch ingestion.
    return count


# COMMAND ----------

# Seed the standard ingest folder with an initial batch of files.
file_counter = add_data(file_counter)

# COMMAND ----------

# Seed the SNS ingest folder with one initial file.
file_counter_sns = move_file_sns(file_counter_sns)

# COMMAND ----------

# Define a cleanup helper to reset demo database and source folders.
def clean_up():
    # Drop the demo database and all contained objects.
    spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")
    # Remove all files from the standard ingest folder.
    dbutils.fs.rm(cloud_storage_path + "/ingest/", True)
    # Remove all files from the SNS ingest folder.
    dbutils.fs.rm(cloud_storage_path + "/ingest_sns/", True)
    # Re-seed standard ingest with initial demo files.
    file_counter = add_data(1)
    # Reset SNS counter to its initial value.
    file_counter_sns = 1
