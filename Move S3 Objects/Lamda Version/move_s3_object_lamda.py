import boto3
from datetime import datetime
import os
import logging

# Setting up the Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
logger.info("Successfully connected to S3 client.")

# Fetching Bucket, Source Path and Target Path
bucket = os.environ['BUCKET_NAME']
src_path = os.environ['SOURCE_PATH']
tgt_path = os.environ['TARGET_PATH']
use_case_id = os.environ['USE_CASE_ID']
logger.info("Successfully fetched Environment Variables.")
    
# Function to create the new Batch ID  
def createBatchID(use_case_id):
    
    now = datetime.now()
    
    timestamp = now.strftime("%y%m%d%H%M%S%f")[:-3]
    
    batch_id = f"{timestamp}{use_case_id}"
    
    logger.info(f"All the files will be moved under Batch_ID: {batch_id}.")
    
    return batch_id


def moveFilesToBeProcessed(bucket, src_prefix, target_prefix):
    
    response = s3_client.list_objects_v2(
        Bucket = bucket,
        Prefix = src_prefix
    )
    
    batch_id = createBatchID(use_case_id)
    
    if 'Contents' in response:
        for obj in response['Contents']:
              
            if len(obj['Key']) > 0:
                
                # Splitting the Key into a list to take only the Files Name
                src_prefixes = obj['Key'].split("/")
                
                file_name = src_prefixes[-1]
                
                if file_name != '':
                    
                    logger.info(f"Moving the file: {file_name}.")
                    
                    # Preparing the Destination Path with new Batch ID
                    dest_key = f"{target_prefix}/{batch_id}/{file_name}"
                    
                    # Copying the File from Source to Destination S3 folder
                    s3_client.copy_object(
                        Bucket = bucket,
                        CopySource = {'Bucket': bucket, 'Key': obj['Key']},
                        Key = dest_key
                    )
                    
                    # Deleting the File from source
                    s3_client.delete_object(
                        Bucket = bucket,
                        Key = obj['Key']
                    )
                    
                    logger.info(f"Successfully moved the file: {file_name}.")
                    
# Entry pint for the Lamda Function                  
def lambda_handler(event, context):
    try:
        moveFilesToBeProcessed(bucket, src_path, tgt_path)
    except Exception as e:
        print("Unable to move files. Please check error: ", e)