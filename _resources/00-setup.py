# Databricks notebook source
# Create a widget to control whether all demo data should be reset.
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
# Create a widget to define the database name prefix for this user/session.
dbutils.widgets.text("db_prefix", "workshop", "Database prefix")
#dbutils.widgets.text("cloud_storage_path", "s3://{bucket_name}", "S3 Bucket")
#dbutils.widgets.text("region_name", "ap-southeast-2", "AWS Region")
#dbutils.widgets.text("stack", "cfn-workspace", "CFN Stack")
# Create a widget to select the target catalog.
dbutils.widgets.text("catalog", "db_workshop", "Catalog")

# COMMAND ----------

# Run shared configuration notebook to load common workshop settings.
# MAGIC %run ../_resources/01-config

# COMMAND ----------

# Build the cloud storage path from Spark configuration values.
cloud_storage_path = 's3://' + spark.conf.get("da.workshop_bucket") #dbutils.widgets.get("cloud_storage_path")
# Print the resolved cloud storage path for visibility.
print("cloud_storage_path :"+cloud_storage_path)
# Persist the cloud storage path in Spark configuration for downstream notebooks.
spark.conf.set("da.cloud_storage_path", cloud_storage_path)

# COMMAND ----------

# Read reset choice from the widget for conditional setup logic.
reset_all_data = dbutils.widgets.get("reset_all_data")

# COMMAND ----------

# Import regex helpers used to sanitize the current username into a valid database suffix.
import re
# Read the current notebook user from Databricks context tags.
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Strip email domain if present to keep only the local user part.
if current_user.rfind('@') > 0:
    # Keep the substring before '@' when user appears as an email address.
    current_user_no_at = current_user[:current_user.rfind('@')]
else:
    # Use the raw username when no domain separator is present.
    current_user_no_at = current_user
# Replace non-word characters so the database name stays SQL-safe.
current_user_no_at = re.sub(r'\W+', '_', current_user_no_at)


# COMMAND ----------

# Read the configured database prefix from widget input.
db_prefix = dbutils.widgets.get("db_prefix")
# Build a user-scoped database name to avoid collisions across workshop users.
dbName = db_prefix + "_" + current_user_no_at
# Print the generated database name for debugging/validation.
print(dbName)

# COMMAND ----------

# Define helper to switch catalog and create schema/database if missing.
def use_and_create_db(catalog, dbName, cloud_storage_path = None):
    # Print the catalog selection command for traceability.
    print(f"USE CATALOG `{catalog}`")
    #print(f"""create schema if not exists `{dbName}` MANAGED LOCATION '{cloud_storage_path}/tables' """)
    # Set the active catalog for subsequent SQL commands.
    spark.sql(f"USE CATALOG `{catalog}`")
    # Create schema/database with managed storage location when absent.
    spark.sql(f"""create schema if not exists `{dbName}` MANAGED LOCATION '{cloud_storage_path}/tables' """)


# COMMAND ----------

# Drop and recreate lab schema when reset is requested.
if reset_all_data:
    # Remove the existing database and all objects to start from a clean state.
    spark.sql(f"DROP DATABASE IF EXISTS `{dbName}` CASCADE")

# Read target catalog selection from widget.
current_catalog = dbutils.widgets.get("catalog")
# Collect available catalogs from the metastore.
catalogs = [r['catalog'] for r in spark.sql("SHOW CATALOGS").collect()]
# Detect non-Unity-Catalog environments and fallback to hive metastore.
if len(catalogs) == 1 and catalogs[0] in ['hive_metastore', 'spark_catalog']:
    # Inform user that Unity Catalog appears unavailable.
    print(f"UC doesn't appear to be enabled")
    # Use hive metastore as default fallback catalog.
    catalog = "hive_metastore"
else:
    # Create the selected catalog if it does not already exist.
    if current_catalog not in catalogs:
        # Ensure chosen catalog exists before creating schema.
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {current_catalog}")
    # Use requested catalog in UC-enabled environments.
    catalog = current_catalog
# Create/use the target schema in the selected catalog and storage location.
use_and_create_db(catalog, dbName, cloud_storage_path)

# Print resolved cloud storage location used by this setup.
print(f"using cloud_storage_path {cloud_storage_path}")
# Print the final catalog.database context for downstream notebooks.
print(f"using catalog.database `{catalog}`.`{dbName}`")
# Persist selected catalog in Spark configuration.
spark.conf.set("da.catalog", catalog)
# Persist selected database name in Spark configuration.
spark.conf.set("da.dbName", dbName)
# Apply workspace-wide grants when operating in Unity Catalog.
if catalog not in ['hive_metastore', 'spark_catalog']:
    #cloud_storage_path+="_"+catalog
    # Attempt to grant create/usage permissions to account users.
    try:
        # Grant schema creation and usage rights on the workshop database.
        spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{dbName} TO `account users`")
        # Transfer schema ownership to account users for collaborative access.
        spark.sql(f"ALTER SCHEMA {catalog}.{dbName} OWNER TO `account users`")
    except Exception as e:
        # Log grant failures without blocking setup completion.
        print("Couldn't grant access to the schema to all users:" + str(e))

# Retry setting active schema because catalog initialization may be eventually consistent.
for i in range(10):
    # Attempt to switch to the target catalog and schema.
    try:
        # Set the active schema context for subsequent notebook commands.
        spark.sql(f"""USE `{catalog}`.`{dbName}`""")
        # Exit retry loop once schema selection succeeds.
        break
    except Exception as e:
        # Wait briefly before retrying setup context initialization.
        time.sleep(1)
        # Re-raise error after final retry to surface setup failure.
        if i >= 9:
            raise e

