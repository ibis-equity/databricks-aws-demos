-- Databricks notebook source
-- MAGIC %run ../_resources/01-config

-- COMMAND ----------

-- Create a Unity Catalog connection object for the external RDS MySQL database.
create connection rds_mysql type mysql
options (
  host '${da.rds_endpoint}',
  port '3306',
  user 'labuser',
  password secret('q_fed','mysql')
);


-- COMMAND ----------

-- List all configured external connections to verify creation succeeded.
show connections;

-- COMMAND ----------

-- Show detailed connection metadata and options for troubleshooting.
describe connection extended rds_mysql;

-- COMMAND ----------

-- Create a foreign catalog that maps MySQL objects through the named connection.
create foreign catalog mysql_external using connection rds_mysql


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Now use SQL Warehouse to query##
