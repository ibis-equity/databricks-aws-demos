# Databricks notebook source
# DBTITLE 1,Create a DynamoDB Table using boto3
# Import AWS SDK for Python to interact with DynamoDB services.
import boto3
# Initialize a DynamoDB client in the target AWS region.
dynamodb = boto3.client('dynamodb',region_name='ap-southeast-2')
# Define the DynamoDB table name used throughout this notebook.
table_name = 'LookUpTable'
# Create the DynamoDB table with composite primary key and provisioned throughput.
table = dynamodb.create_table(
    TableName=table_name,
    KeySchema=[
        {
            'AttributeName': 'brand',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'model',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'brand',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'model',
            'AttributeType': 'S'
        },

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }
)

# COMMAND ----------

# DBTITLE 1,Insert record into Table
# Reuse the target table name for item insertion.
table_name = 'LookUpTable'
# Insert a sample item into the lookup table.
dynamodb.put_item(
    TableName=table_name,
    Item={
        'brand': {'S': 'BMW'},
        'model': {'S': '3 Series'}
    }
)

# COMMAND ----------

# DBTITLE 1,Retrieve record from Table
# Define the composite key used to fetch a specific item.
key = {'brand': {'S': 'BMW'},'model':{'S':'3 Series'}}

# Retrieve the matching item from DynamoDB by primary key.
response = dynamodb.get_item(TableName=table_name, Key=key)

# Print the raw DynamoDB item payload returned by the get call.
print(response['Item'])


# COMMAND ----------

# DBTITLE 1,Retrieve record into Spark Dataframe
# Extract the DynamoDB item dictionary from the API response.
item = response['Item']
# Convert DynamoDB typed attributes into a plain Python dictionary.
item_dict = {k:v['S'] if 'S' in v else v['N'] for k,v in item.items()}

# Create a Spark DataFrame from the normalized item dictionary.
df = spark.createDataFrame([item_dict])

# Display the DataFrame in the Databricks notebook output.
df.display()

# COMMAND ----------

# DBTITLE 1,Clean Up
# Delete the demo DynamoDB table to clean up created resources.
response = dynamodb.delete_table(TableName='LookUpTable')
