# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data from RDS to Delta Lake using AWS Database Migration Service (DMS)
# MAGIC
# MAGIC This notebook shows you how to import data from output by DMS. We will show full load and CDC
# MAGIC
# MAGIC https://aws.amazon.com/dms/

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://cms.databricks.com/sites/default/files/inline-images/db-300-blog-img-1.png"  width="600"/>

# COMMAND ----------

# Create a notebook widget to control whether demo data is reset.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")


# COMMAND ----------

# Run shared lab setup notebook and pass through the reset widget value.
# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# Run helper notebook that prepares DMS/CDC-related demo artifacts.
# MAGIC %run ../_resources/02-dms-cdc-data

# COMMAND ----------

# Read RDS endpoint hostname from Spark configuration.
database_host = spark.conf.get("da.rds_endpoint")
# Define the source MySQL database name.
database_name = 'demodb'
# Define the source MySQL port.
database_port = "3306"
# Read database username from Spark configuration.
username = spark.conf.get("da.rds_user")
# Read database password from Spark configuration.
password = spark.conf.get("da.rds_password")

# Build the JDBC URL used for Spark JDBC reads.
jdbcUrl = f"jdbc:mysql://{database_host}:{database_port}/{database_name}"
# Define JDBC connection properties passed to Spark read/write methods.
connectionProperties = {
  "user" : username,
  "password" : password,
  "ssl" : "true"   # SSL for secure connection
}
# Print JDBC URL for quick verification during lab execution.
print(jdbcUrl)

# COMMAND ----------

# Read the remote `customers` table from RDS through JDBC.
remote_table = spark.read.jdbc(url=jdbcUrl, table="customers", properties=connectionProperties)

# Display imported RDS records in the notebook output.
remote_table.display()
# Print schema of the imported RDS table.
remote_table.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lab 4: DMS for Full and CDC Loads
# MAGIC ### 1. Introduction to AWS Database Migration Service (DMS)
# MAGIC
# MAGIC AWS DMS is a tool that can move data to and from the most widely used commercial and open-source databases. It supports homogeneous migrations as well as migrations between different database systems. In this lab, we have pre-configured DMS tasks to migrate data from an RDS MySQL database to S3.
# MAGIC
# MAGIC ### 2. Starting a Full Load Migration Task with AWS DMS
# MAGIC
# MAGIC In this section, you will start a pre-configured Full Load migration task on DMS. Follow the steps below:
# MAGIC
# MAGIC - Log into your AWS console and navigate to the DMS dashboard.
# MAGIC - Go to the DMS Tasks page and find the Full Load task.
# MAGIC - Click on the "Start/Resume" button to initiate the task.
# MAGIC - Monitor the task progress from the task monitoring section.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reading Full Load CSV Data into Databricks
# MAGIC
# MAGIC The Full Load DMS task will create CSV files in a specified S3 bucket. We will now read this data into Databricks.

# COMMAND ----------

# DBTITLE 0,Full Load
# Preview raw full-load CSV file created by DMS before ingestion.
# MAGIC %sql
# MAGIC --Check the file before loading
# MAGIC SELECT * FROM csv.`${da.cloud_storage_path}/dms-output/demodb/customers/LOAD00000001.csv.gz`

# COMMAND ----------

# Read CSV files from S3 into a DataFrame
# Load DMS full-load CSV output from cloud storage into a DataFrame.
df = spark.read.format('csv').options(header='true', inferSchema='true').load(cloud_storage_path+"/dms-output/demodb/customers/LOAD00000001.csv.gz")

# Write the DataFrame into a Delta table
# Persist the full-load DataFrame to a managed Delta table.
df.write.format("delta").saveAsTable("customers")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 4. Data Consistency Check between RDS and Delta Table
# MAGIC
# MAGIC Now, we will perform a data consistency check between the original data in RDS and the new Delta table.
# MAGIC
# MAGIC First, we'll read data from RDS via JDBC.

# COMMAND ----------

# Read the authoritative RDS `customers` table for consistency validation.
df_rds = spark.read.jdbc(url=jdbcUrl, table="customers", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Then, compare the count of rows in both DataFrames.

# COMMAND ----------

# Assert row counts match between Delta and RDS as a basic consistency check.
assert df.count() == df_rds.count(), "Data is not consistent between RDS and Delta table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now lets generate some CDC data 

# COMMAND ----------

# Execute generator notebook to create fresh CDC events in the source system.
# MAGIC %run ../_resources/02-dms-cdc-data-generator

# COMMAND ----------

# MAGIC %md
# MAGIC Run the below command to check that the CDC data is Landing. We will use this in our Next Lab using Delta Live Tables

# COMMAND ----------

# DBTITLE 0,CDC Data
# Inspect landed CDC CSV output that will be used by downstream labs.
# MAGIC %sql
# MAGIC SELECT * FROM csv.`${da.cloud_storage_path}/dms-cdc-output/demodb/customers/`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Best Practices
# MAGIC
# MAGIC When using AWS DMS and Databricks, there are several best practices to consider:
# MAGIC
# MAGIC - **Monitor your DMS tasks**: Regularly check the task monitoring section in DMS to ensure your tasks are running as expected.
# MAGIC - **Validate your data**: After performing a data migration, always validate the migrated data to ensure consistency.
# MAGIC - **Secure your connections**: When connecting to databases (like in JDBC), always secure your credentials. Consider using Databricks secrets for storing and using sensitive information.
# MAGIC - **Handle schema changes**: When doing CDC, consider how you will handle schema changes in your source database. This could affect the format of your data files and the structure of your Delta table.
