import boto3

import argparse
import json

def get_request_s3(request_bucket):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=request_bucket)

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Parse command-line arguments for consumer program")

    parser.add_argument('-rb', '--request-bucket', type=str, help='Name of S3 bucket containing widget requests')
    parser.add_argument('-rq', '--request-queue', type=str, help='URL of queue containing widget requests')
    parser.add_argument('-wb', '--widget-bucket', type=str, help='Name of S3 bucket to store widgets')
    parser.add_argument('-dwt', 'dynamodb-widget-table', type=str, help='DynamoDB table to store widgets')

    args = parser.parse_args()
