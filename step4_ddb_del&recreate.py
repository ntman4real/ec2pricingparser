import boto3

def ddb_decrease(event,context):
    dynamodb = boto3.client(service_name='dynamodb')
    response = dynamodb.update_table(TableName='ec2_pricing',ProvisionedThroughput={
            'ReadCapacityUnits': 25,
            'WriteCapacityUnits': 25
        }
    )