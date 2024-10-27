import boto3

import time

import argparse
import json

import pandas as pd
from jsonschema import validate, ValidationError

schema = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "pattern": "create|delete|update"
        },
        "requestId": {
            "type": "string"
        },
        "widgetId": {
            "type": "string"
        },
        "owner": {
            "type": "string",
            "pattern": "[A-Za-z ]+"
        },
        "label": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "otherAttributes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "value": {
                        "type": "string"
                    }
                },
                "required": [
                    "name",
                    "value"
                ]
            }
        }
    },
    "required": [
        "type",
        "requestId",
        "widgetId",
        "owner"
    ]
}

class S3_Bucket:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket_name = bucket_name

    def list_objects(self):
        return self.s3.list_objects_v2(Bucket=self.bucket_name)

    def get_object(self, key):
        return self.s3.get_object(Bucket=self.bucket_name, Key=key)
    
    def widget_create(self, request):
        key = f'widgets/{request["owner"].replace(' ', '-').lower()}/{request["widgetId"]}'
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=json.dumps(request))

    def widget_delete(self, request):
        # Widget deletion logic
        pass

    def widget_update(self, request):
        # Widget update logic
        pass

class DynamoDB_Table:
    def __init__(self, table_name):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)

    def get_table(self, table_name):
        return self.dynamodb.Table(table_name)
    
    def widget_create(self, request):
        if 'widgetId' in request:
            request['id'] = request.pop('widgetId')
        other_attributes = {attr['name']: attr['value'] for attr in request['otherAttributes']}
        request.update(other_attributes)
        del request['otherAttributes']
        self.table.put_item(Item=request)

    def widget_delete(self, request):
        # Widget deletion logic
        pass

    def widget_update(self, request): 
        # Widget update logic
        pass

def process_request(request, args):
    if args.widget_bucket is not None:
        s3_client = S3_Bucket(args.widget_bucket)
        if request['type'] == 'create':
            s3_client.widget_create(request)
        elif request['type'] == 'delete':
            s3_client.widget_delete(request)
        elif request['type'] == 'update':
            s3_client.widget_update(request)

    if args.dynamodb_widget_table is not None:
        dynamodb_client = DynamoDB_Table(args.dynamodb_widget_table)
        if request['type'] == 'create':
            dynamodb_client.widget_create(request)
        elif request['type'] == 'delete':
            dynamodb_client.widget_delete(request)
        elif request['type'] == 'update':
            dynamodb_client.widget_update(request)

def get_request(request_bucket):
    requests = request_bucket.list_objects()
    if requests['KeyCount'] == 0:
        return None
    nextKey = min(obj['Key'] for obj in requests['Contents'])
    nextRequest = request_bucket.get_object(nextKey)
    if nextRequest['ContentLength'] == 0:
        request_bucket.s3.delete_object(Bucket=request_bucket.bucket_name, Key=nextKey)
        return None
    nextRequestBody = nextRequest['Body'].read().decode('utf-8')    
    try:
        validate(instance=json.loads(nextRequestBody), schema=schema)
    except ValidationError as e:
        print(f"Validation error: {e.message}")
        request_bucket.s3.delete_object(Bucket=request_bucket.bucket_name, Key=nextKey)
        return None

    request = json.loads(nextRequestBody)
    request_bucket.s3.delete_object(Bucket=request_bucket.bucket_name, Key=nextKey)
    return request

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Command-line arguments for consumer program")

    parser.add_argument('-rb', '--request-bucket', type=str, help='Name of S3 bucket containing widget requests')
    parser.add_argument('-wb', '--widget-bucket', type=str, help='Name of S3 bucket to store widgets')
    parser.add_argument('-dwt', '--dynamodb-widget-table', type=str, help='DynamoDB table to store widgets')

    args = parser.parse_args()

    request_bucket = S3_Bucket(args.request_bucket)

    while True:
        request = get_request(request_bucket)
        if request is not None:
            process_request(request, args)
        else:
            time.sleep(0.1)
