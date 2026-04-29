# Databricks notebook source
# Import AWS SDK to access CloudFormation and Secrets Manager APIs.
import boto3
# Import AWS client exception type for Secrets Manager error handling.
from botocore.exceptions import ClientError
# Import JSON parsing utilities for decoding secret payloads.
import json

# Define helper to fetch and parse a password from AWS Secrets Manager.
def get_secret(region_name,secret_name):
    # Create a boto3 session scoped to the current runtime credentials.
    session = boto3.session.Session()
    # Build a Secrets Manager client in the requested AWS region.
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # Attempt to fetch the secret value by secret identifier.
    try:
        # Call Secrets Manager to retrieve the secret JSON payload.
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # Re-raise AWS API errors so callers can decide how to handle failure.
        raise e

    # Extract the decrypted secret string from the API response.
    secret = get_secret_value_response['SecretString']
    # Parse JSON and select the password field expected by this workshop.
    password = json.loads(secret)["password"]
    # Return the resolved password string.
    return password

# COMMAND ----------

# Import HTTP client used to query EC2 instance metadata service.
import requests

# Define helper to discover current AWS region from instance metadata.
def get_region():
    # Set IMDSv2 token endpoint URL.
    token_url = "http://169.254.169.254/latest/api/token"
    # Set token TTL header required for IMDSv2 authentication.
    token_headers = {"X-aws-ec2-metadata-token-ttl-seconds": "21600"}

    # Request a temporary metadata token from IMDSv2.
    token_response = requests.put(token_url, headers=token_headers)

    # Extract token string from the token response body.
    token = token_response.text

    # Set metadata URL that returns the current instance region.
    metadata_url = "http://169.254.169.254/latest/meta-data/placement/region"
    # Attach the IMDSv2 token when requesting metadata.
    metadata_headers = {"X-aws-ec2-metadata-token": token}

    # Query instance metadata service for region name.
    metadata_response = requests.get(metadata_url, headers=metadata_headers)

    # Return the resolved AWS region string.
    return metadata_response.text


# COMMAND ----------

# Import boto3 again in this notebook cell for standalone cell execution support.
import boto3
# Define helper to read CloudFormation outputs and publish them to Spark config.
def get_cfn():
    # Create CloudFormation client in the current runtime AWS region.
    client = boto3.client('cloudformation',region_name=get_region())
    # Retrieve available CloudFormation stacks in the account/region.
    response = client.describe_stacks()#StackName=dbutils.widgets.get("stack"))
    # Initialize dictionary to collect relevant stack outputs.
    cfn_outputs = {}
    # Iterate through all discovered stacks.
    for stack in response['Stacks']:
        # Read optional outputs section from each stack.
        outputs = stack.get('Outputs', [])
        # Continue processing only when outputs are present.
        if outputs:

            # Define output keys required by this workshop setup.
            desired_output_keys = ['DatabrickWorkshopBucket', 'RDSendpoint', 'RDSsecret']
            

            # Scan outputs and keep only keys used by the notebook.
            for output in outputs:
                # Read CloudFormation output key name.
                output_key = output['OutputKey']
                # Save output value if key matches required set.
                if output_key in desired_output_keys:
                    cfn_outputs[output_key] = output['OutputValue']

            # Read workshop S3 bucket output used for data paths.
            workshop_bucket = cfn_outputs['DatabrickWorkshopBucket']
            # Configure RDS settings only when RDS outputs are available.
            if 'RDSendpoint' in cfn_outputs:
                # Read RDS endpoint from CloudFormation outputs.
                rds_endpoint = cfn_outputs['RDSendpoint']
                # Set known workshop username for RDS access.
                rds_user = 'labuser'
                # Resolve RDS password from referenced Secrets Manager secret.
                rds_password = get_secret(get_region(),cfn_outputs['RDSsecret'])
            else:
                # Set placeholder endpoint when no RDS stack output exists.
                rds_endpoint = 'None'
                # Set placeholder username when no RDS output exists.
                rds_user = 'None'
                # Set placeholder password when no RDS output exists.
                rds_password = 'None'
            
            # Persist workshop bucket value to Spark config for downstream notebooks.
            spark.conf.set("da.workshop_bucket",workshop_bucket)
            # Persist RDS endpoint value to Spark config.
            spark.conf.set("da.rds_endpoint",rds_endpoint)
            # Persist RDS username value to Spark config.
            spark.conf.set("da.rds_user",rds_user)
            # Persist RDS password value to Spark config.
            spark.conf.set("da.rds_password",rds_password)

            # Print resolved infrastructure values for verification.
            print(f"""
            S3 Bucket:                  {cfn_outputs['DatabrickWorkshopBucket']}
            RDS End Point:              {rds_endpoint}
            RDS User:                   {rds_user}
            RDS Password:               {rds_password}
            """)

# COMMAND ----------

# Execute CloudFormation discovery and publish outputs into Spark configuration.
get_cfn()

# COMMAND ----------

# Import utilities for inspecting installed packages in the current environment.
import pkg_resources
# Import subprocess helper to execute pip installation commands.
import subprocess
# Import sys to reference current Python executable for pip execution.
import sys

# Read all currently installed packages from the active Python environment.
installed_packages = pkg_resources.working_set
# Build a sorted list of installed package names.
installed_packages_list = sorted(["%s" % (i.key)
   for i in installed_packages])
#print(installed_packages_list)
# Install Databricks SDK only when it is not already available.
if 'databricks-sdk' not in installed_packages_list:
    # Run pip install using current interpreter to ensure correct environment target.
    subprocess.check_call([sys.executable, "-m", "pip", "install", "databricks-sdk"])

# COMMAND ----------

# Import workspace client used to create and manage Databricks secrets.
from databricks.sdk import WorkspaceClient
# Import workspace service models for ACL permission constants.
from databricks.sdk.service import workspace
# Instantiate a Databricks workspace client using notebook credentials.
w = WorkspaceClient()

# Define helper to create secret scope, secret value, and permissions for query federation.
def create_secret():
    # Define secret scope name used by SQL query federation demo.
    scope_name= 'q_fed'
    # Define secret key name that stores MySQL password.
    key_name = 'mysql'
    # Create secret scope in workspace.
    w.secrets.create_scope(scope=scope_name)
    # Store current RDS password in the secret scope.
    w.secrets.put_secret(scope=scope_name, key=key_name, string_value=spark.conf.get("da.rds_password"))
    # Grant MANAGE permission on scope to workspace users.
    w.secrets.put_acl(scope=scope_name, permission=workspace.AclPermission.MANAGE, principal="users")

# Define helper to ensure the required secret scope exists before use.
def check_secret():
    # List existing secret scopes in the workspace.
    scopes = w.secrets.list_scopes()
    # Iterate through scopes to find expected query federation scope.
    for scope in scopes:
        # Exit early when required scope already exists.
        if scope.name == 'q_fed':
            return

    # Create scope and secret if not found in existing scopes.
    create_secret()


# COMMAND ----------

# Ensure required secret scope/value exists for downstream SQL federation setup.
check_secret()
