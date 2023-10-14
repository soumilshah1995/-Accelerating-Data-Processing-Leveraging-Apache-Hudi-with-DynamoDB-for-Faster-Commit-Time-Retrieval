import json
import json
import json
import boto3
import time
from datetime import datetime


# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')


def create_dynamodb_table(table_name):
    try:
        response = dynamodb.describe_table(TableName=table_name)
    except dynamodb.exceptions.ResourceNotFoundException:
        # The table doesn't exist, so create it with pay-per-request capacity
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'TableName',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'CommitTime',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'TableName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'CommitTime',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        # Wait for the table to be created
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)


def insert_to_dynamodb(dynamodb_table_name, tableName, commitTime, basePath):
    # Generate the ingestion timestamp and date components
    ingestion_timestamp = int(time.time() * 1000)
    ingestion_datetime = datetime.utcfromtimestamp(ingestion_timestamp / 1000)
    year = ingestion_datetime.strftime('%Y')
    month = ingestion_datetime.strftime('%m')
    day = ingestion_datetime.strftime('%d')
    hour = ingestion_datetime.strftime('%H')
    minute = ingestion_datetime.strftime('%M')

    # Create the DynamoDB item as a dictionary
    dynamodb_item = {
        'TableName': {'S': tableName},
        'CommitTime': {'S': commitTime},
        'BasePath': {'S': basePath},
        'IngestionTimestamp': {'N': str(ingestion_timestamp)},
        'Year': {'S': year},
        'Month': {'S': month},
        'Day': {'S': day},
        'Hour': {'S': hour},
        'Minute': {'S': minute},

    }

    # Insert the item into DynamoDB
    dynamodb.put_item(TableName=dynamodb_table_name, Item=dynamodb_item)


def lambda_handler(event, context=None):
    event = event['body']
    event = json.loads(event)

    # Ensure that the required keys are present in the event
    if 'commitTime' not in event or 'tableName' not in event or 'basePath' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Missing required keys in the event.'})
        }
    else:
        # Extract the values from the event
        commitTime = event['commitTime']
        tableName = event['tableName']
        basePath = event['basePath']
        dynamodb_table_name = "hudi_commits"

        # Ensure the DynamoDB table exists (create if it doesn't)
        # create_dynamodb_table(tableName)

        # Insert data into DynamoDB using the separate function
        insert_to_dynamodb(dynamodb_table_name, tableName, commitTime, basePath)

        # Return a success response
        response = {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data inserted into DynamoDB.'})
        }

        return response


# create_dynamodb_table(table_name="hudi_commits")
