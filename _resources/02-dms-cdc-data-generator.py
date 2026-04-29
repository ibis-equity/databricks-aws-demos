# Databricks notebook source
# Import AWS SDK used to invoke the CDC data generator Lambda.
import boto3
# Resolve current AWS region from shared metadata helper.
region = get_region()
# Create a Lambda client scoped to the resolved region.
client = boto3.client('lambda', region_name=region)
# Invoke CDC data generator Lambda synchronously.
response = client.invoke(
    FunctionName='db-CDCDataGen',
    InvocationType='RequestResponse',  # Set the invocation type as needed
    LogType='Tail',  # Set the log type as needed
    Payload='{}'  # Pass the payload as a string (if required)
)
# Capture the Lambda invocation status code.
status_code = response['StatusCode']
# Decode Lambda response payload into a human-readable string.
response_payload = response['Payload'].read().decode('utf-8')

# Process the response as needed
#print(f"Status code: {status_code}")
# Print Lambda response payload for quick verification.
print(f"Response : {response_payload}")
#print(response)
