import boto3
import json
from boto3.dynamodb.conditions import Key

class PayloadDBHandler:
    def __init__(self, accessKey, accessSecret, regionName, tableName):
        #connect
        self.dbClient = boto3.client('dynamodb',
                                  aws_access_key_id=accessKey,
                                  aws_secret_access_key=accessSecret,
                                  region_name=regionName,
                                  endpoint_url="http://dynamodb.{}.amazonaws.com".format(regionName))
        self.dbResource = boto3.resource('dynamodb',
                                  aws_access_key_id=accessKey,
                                  aws_secret_access_key=accessSecret,
                                  region_name=regionName,
                                  endpoint_url="http://dynamodb.{}.amazonaws.com".format(regionName))

        self.tableName = tableName
        self.table = None

    def table_exists(self):
        return self.tableName in self.dbClient.list_tables()['TableNames']

    def delete_table(self):
        if (self.table_exists()):
            self.dbClient.delete_table(TableName=self.tableName)
            print('Waiting for table deletion.')
            self.dbClient.get_waiter('table_not_exists').wait(TableName=self.tableName)
            print('Table deleted.')
        else:
            print('Table doesn\'t exist and thus doesn\'t need to be deleted.')

    def init_table(self):
        if(not self.table_exists()):
            self.table = self.dbResource.create_table(
                TableName=self.tableName,
                KeySchema=[
                    {
                        'AttributeName': 'payloadID',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'sequence',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'payloadID',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'sequence',
                        'AttributeType': 'N'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 4,
                    'WriteCapacityUnits': 10
                }
            )

            #wait for initialization
            print('Waiting for table initialization.')
            self.table.meta.client.get_waiter('table_exists').wait(TableName=self.tableName)
            print('Table initialized.')
        else:
            self.table = self.dbResource.Table(self.tableName)
            print('Table initialized.')

    def get_payload(self, payloadID, sequence):
        response = self.table.query(
            KeyConditionExpression=Key('sequence').eq(sequence) & Key('payloadID').eq(payloadID)
        )

        if (len(response['Items']) > 0):
            oldPayloadStr = response['Items'][0]['payload']
            oldPayload = json.loads(oldPayloadStr)
            return oldPayload

    def set_payload(self, payloadID, sequence, payload):
        self.table.put_item(
            Item={
                'payloadID': payloadID,
                'sequence': sequence,
                'payload': json.dumps(payload)
            },
        )

    def get_payloads_after_sequence(self, sequence, payloadID):
        response = self.table.query(
            KeyConditionExpression=Key('sequence').gt(sequence) & Key('payloadID').eq(payloadID)
        )
        payloads = []
        for item in response['Items']:
            payloads.append(json.loads(item['payload']))
        return payloads

    def get_lambda_async_results(self, payloadID, sequencesToCheck):
        results = []
        found = []
        for seq in sequencesToCheck:
            #if result has not been fetched yet
            response = self.table.query(
                KeyConditionExpression=Key('sequence').eq(seq) & Key('payloadID').eq(payloadID)
            )
            if(len(response['Items'])>0):
                results.append(json.loads(response['Items'][0]['payload']))
                found.append(seq)
        return results, found

    def delete_payloads_before_sequence(self, sequence, payloadID):
        for i in range(sequence):

            if not i%100:
                print('Deleted sequences {}-{}'.format(0, i-1))

            self.dbClient.batch_write_item(
                RequestItems={
                    self.tableName: [
                        {
                            'DeleteRequest': {
                                'Key': {
                                    'payloadID': {'S': payloadID},
                                    'sequence': {'N': str(i)}
                                }
                            }
                        }
                    ]
                }
            )

# import os,time,sys
# from dotenv import load_dotenv, find_dotenv
# load_dotenv(find_dotenv())
# accessKey = os.environ.get("AWS_ACCESS_KEY")
# accessSecret = os.environ.get("AWS_ACCESS_SECRET")
# regionName = os.environ.get("REGION_NAME")
# tableName = 'AltCoinGridSearchResults'
# payloadID = 'test'
# payloadHandler = PayloadDBHandler(accessKey, accessSecret, regionName, tableName)
# payloadHandler.init_table()
# payloadHandler.delete_payloads_before_sequence(sys.maxsize, payloadID)
# payloadHandler.set_payload(payloadID, 0, {'test':123})
# print(payloadHandler.get_payloads_after_sequence(-1, payloadID))