# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Load data from MySQL to Delta Lake
# MAGIC
# MAGIC This notebook shows you how to import data from JDBC MySQL databases into a Delta Lake table using Python.
# MAGIC
# MAGIC https://docs.databricks.com/external-data/jdbc.html

# COMMAND ----------

# Create a notebook widget to optionally reset demo data before running setup.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# Execute shared setup notebook and pass through the reset widget value.
# MAGIC %run ../_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lab 3: Consuming Amazon RDS
# MAGIC **1. Connecting to Amazon RDS MySQL Database**
# MAGIC
# MAGIC To interact with Amazon RDS from Databricks, we need to establish a connection using JDBC. We create a JDBC URL, which includes the RDS endpoint, port, and database name. Along with this, we specify the connection properties which include the username, password, and driver details.
# MAGIC
# MAGIC ```
# MAGIC jdbcHostname = "<RDS-endpoint>"
# MAGIC jdbcDatabase = "<database-name>"
# MAGIC jdbcPort = 3306
# MAGIC jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"
# MAGIC
# MAGIC connectionProperties = {
# MAGIC   "user" : "<username>",
# MAGIC   "password" : "<password>",
# MAGIC   "driver" : "com.mysql.jdbc.Driver",
# MAGIC   "ssl" : "true"   # SSL for secure connection
# MAGIC }
# MAGIC ```

# COMMAND ----------

# Read the RDS endpoint hostname from Spark configuration.
jdbcHostname = spark.conf.get("da.rds_endpoint")
# Set the source database name to use for JDBC operations.
jdbcDatabase = 'demodb'
# Define the MySQL port used by the RDS instance.
jdbcPort = "3306"
# Read the JDBC username from Spark configuration.
username = spark.conf.get("da.rds_user")
# Read the JDBC password from Spark configuration.
password = spark.conf.get("da.rds_password")

# Build the JDBC URL string required for Spark JDBC read/write APIs.
jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}"

# Define JDBC connection properties passed to Spark JDBC methods.
connectionProperties = {
  "user" : username,
  "password" : password,
  "ssl" : "true"   # SSL for secure connection
}
# Print the final JDBC URL for quick verification.
print(jdbcUrl)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The full URL printed out above should look something like:
# MAGIC
# MAGIC ```
# MAGIC jdbc:postgresql://localhost:3306/my_database
# MAGIC jdbc:mysql://localhost:3306/my_database
# MAGIC ```
# MAGIC
# MAGIC ### Check connectivity
# MAGIC
# MAGIC Depending on security settings for your Postgres database and Databricks workspace, you may not have the proper ports open to connect.
# MAGIC
# MAGIC Replace `<database-host-url>` with the universal locator for your Postgres implementation. If you are using a non-default port, also update the 5432.
# MAGIC
# MAGIC Run the cell below to confirm Databricks can reach your Postgres database.

# COMMAND ----------

# Run a shell-level network connectivity check to the database host/port.
# MAGIC %sh
# MAGIC nc -vz "<database-host-url>" 3306

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Writing to an RDS Table
# MAGIC We can write data from a Spark DataFrame back to an RDS table using the `write.jdbc` function. We need to specify the JDBC URL, the name of the destination table, the write mode (overwrite, append, etc.), and the connection properties.

# COMMAND ----------

#create a dataframe to write to the Database
# Create an in-memory sample DataFrame that will be written to RDS.
df = spark.createDataFrame( [ ("Bilbo",     50),
                                  ("Gandalf", 1000), 
                                  ("Thorin",   195),  
                                  ("Balin",    178), 
                                  ("Kili",      77),
                                  ("Dwalin",   169), 
                                  ("Oin",      167), 
                                  ("Gloin",    158), 
                                  ("Fili",      82), 
                                  ("Bombur",  None)
                                ], 
                                ["name", "age"] 
                              )

# COMMAND ----------

# DBTITLE 0,Write Dataframe to RDS
# Write the sample DataFrame to the RDS `people` table, replacing existing data.
df.write.jdbc(url=jdbcUrl, table="people", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Reading from an RDS Table
# MAGIC To read from an RDS table, we can use the `spark.read.jdbc` function. This function takes the JDBC URL, table name, and connection properties as parameters. We can either perform a full table read or a filtered read. For a filtered read, we specify the SQL query as the table parameter.

# COMMAND ----------

# DBTITLE 0,Read RDS Table
# Full table read
# Read the full `people` table from RDS into a Spark DataFrame.
df = spark.read.jdbc(url=jdbcUrl, table="people", properties=connectionProperties)

# Display the loaded RDS data in notebook output.
df.display()
# Print inferred schema of the loaded JDBC DataFrame.
df.printSchema()

# COMMAND ----------

# Filtered read
# Define the maximum number of rows to fetch in the filtered query.
n = 5 # Number of rows to take
# Build a JDBC subquery string for a limited ordered read.
sql = "(SELECT * FROM people order by age LIMIT {0} ) AS tmp".format(int(n))
# Read filtered rows from RDS using a SQL subquery as the source table.
df = spark.read.jdbc(url=jdbcUrl, table=sql, properties=connectionProperties)

# Display filtered rows returned by the JDBC subquery.
df.display()
# Print schema of the filtered result DataFrame.
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Create a Delta table
# MAGIC
# MAGIC Delta Lake is a powerful technology that brings reliability, performance, and lifecycle management to data lakes. We can convert our DataFrame to a Delta table to leverage these benefits. The Delta table can be queried using SQL and provides ACID transaction guarantees.

# COMMAND ----------

# Set the destination Delta table name used for Spark-managed storage.
target_table_name = "`people`"
# Persist the DataFrame into a Delta table, replacing existing content.
df.write.mode("overwrite").saveAsTable(target_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This table will persist across cluster sessions, notebooks, and personas throughout your organization.
# MAGIC
# MAGIC The code below demonstrates querying this data with Python and SQL.

# COMMAND ----------

# Query the Delta table with SQL to validate persisted results.
# MAGIC %sql
# MAGIC SELECT * FROM `people`

# COMMAND ----------

# Load the Delta table as a DataFrame and display it in the notebook.
display(spark.table(target_table_name))

# COMMAND ----------


