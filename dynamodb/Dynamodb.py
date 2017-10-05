import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


class Dynamo:
    client = boto3.client("dynamodb", endpoint_url="http://localhost:8000")
    resource = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")

    def insert_record_client(self, data, tablename):
        response = self.client.put_item(
            TableName=tablename,
            Item=data
        )
        return response

    def delete_record(self, primary_key, tablename):
        pass

    def scan_table_client(self, primary_key, tablename):
        """this needs improvement"""
        response = self.client.scan(
            TableName=tablename,
            IndexName=primary_key,
            Select='ALL_ATTRIBUTES'
        )
        return response

    def scan_table_resource(self, primary_key, tablename):
        """this needs improvement"""
        table = self.resource.Table(tablename)
        response = table.scan(
            TableName=tablename,
            IndexName=primary_key,
            Select='ALL_ATTRIBUTES'
        )
        return response

    def query_gsi(self, gsi, tablename):
        pass

    def query_table_resource(self, index_name, index_value,
                   primary_key_name,
                   attribute_to_get, tablename):
        """I find querying the table using resource much more simpler than querying through client."""
        table = self.resource.Table(tablename)
        try:
            response = table.query(
                IndexName=index_name + '-index',
                KeyConditionExpression=Key(index_name).eq(index_value)
            )
        except ClientError as e:
            print e.response['Error']['Message']
        else:
            items = []
            for _ in range(response['Count']):
                items.append(response['Items'][_][attribute_to_get])
            while True:
                if 'LastEvaluatedKey' in response:
                    response = table.query(
                            IndexName=index_name + '-index',
                            KeyConditionExpression=Key('index_name').eq(index_value),
                            ExclusiveStartKey={
                                primary_key_name: response['LastEvaluatedKey']['primary_key_name'],
                                #sort_key_name: response['LastEvaluatedKey']['sort_key_name']
                            }
                        )
                    for _ in range(response['Count']):
                            items.append(response['Items'][_][attribute_to_get])
                else:
                    break
            return items
        return None
