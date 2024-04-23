
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError
import os


s3_client = boto3.client('s3')
textract = boto3.client('textract')

   




def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


response = s3_client.list_buckets()\

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')
    






def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
 
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


upload_file('C:\\Users\\abhin\\OneDrive\\Documents\\Optab\\Data\\demo.pdf', 'textract-console-us-east-2-ae857a57-435c-46de-ba4c-fa07598fd9d5')


def extract_table(file_name, bucket):
    # Process using S3 object
    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': file_name
            }
        },
        FeatureTypes=["TABLES"]
    )
    # Get the text blocks
    blocks=response['Blocks']
    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)
    if len(table_blocks) <= 0:
        return "NO TABLE FOUND"
    return table_blocks


tb = extract_table('demo.pdf', 'textract-console-us-east-2-ae857a57-435c-46de-ba4c-fa07598fd9d5')

tb = pd.DataFrame(tb)

print(tb.head())
