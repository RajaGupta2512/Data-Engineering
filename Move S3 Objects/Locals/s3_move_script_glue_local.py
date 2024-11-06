import boto3
from datetime import datetime
from time import sleep
import config_file as config_file

s3_client = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='+yGrle0oPUNTP6slh3bdUNqpZF66'
)

config = config_file.config['DEV'] # take from glue job

bucket = config['bucket-name']
src_path = config['source-path']
tgt_path = config['target-path']
use_case_id = config['use_case_id']

def create_batch_ID(use_case_id):
    
    now = datetime.now()
    
    timestamp = now.strftime("%y%m%d%H%M%S%f")[:-3]
    
    batch_id = f"{timestamp}{use_case_id}"
    
    return batch_id


def get_distinct_files(file_list):
    
    distinct_list = []
    
    for file in file_list:
        if file not in distinct_list:
            distinct_list.append(file)
            
    return distinct_list

def get_S3_folders_list(bucket, prefix):
    
    response = s3_client.list_objects_v2(Bucket = bucket, Prefix = prefix)

    file_list = []
    final_distinct_files = []

    for item in response['Contents']:
        
        file_object = item['Key'].split("/")
        
        if 'archive' not in file_object and len(file_object) > 4:
            file_list.append('/'.join(file_object[:-1]))
    
    final_distinct_files = get_distinct_files(file_list)
            
    return final_distinct_files
    

def move_files_to_archive(bucket, src_prefix, target_prefix, batch_id):
    
    response = s3_client.list_objects_v2(
        Bucket = bucket,
        Prefix = src_prefix
    )
    
    if 'Contents' in response:
        for obj in response['Contents']:    
            if len(obj['Key']) > 3:
        
                src_prefixes = obj['Key'].split("/")
                
                if 'archive' not in src_prefixes and len(src_prefixes) > 4 and len(src_prefixes[4]) > 0:
                    
                    src_batch_file = f"{src_prefixes[3]}/{src_prefixes[4]}"
                    
                    dest_key = f"{target_prefix}{batch_id}/{src_batch_file}"
                    
                    s3_client.copy_object(
                        Bucket = bucket,
                        CopySource = {'Bucket': bucket, 'Key': obj['Key']},
                        Key = dest_key
                    )
                    
                    s3_client.delete_object(
                        Bucket = bucket,
                        Key = obj['Key']
                    )
                    
                    print(f"Moved file: {src_prefixes[4]}")
            

def main():
    
    retry_limit = 5
    retry_count = 0
    
    batch_id = create_batch_ID(use_case_id)
    
    while retry_count != retry_limit:
        
        file_list = get_S3_folders_list(bucket, src_path)
        
        if len(file_list) > 0:
            print(f"Files present in these folders will be {file_list} moved.")
            for folder in file_list:
                print(f"Moving the files of folder : {folder}")
                move_files_to_archive(bucket, folder, tgt_path, batch_id)
            break
        else:
            print("No files to move... sleeping for 300 sec")
            sleep(120)
            retry_count += 1

try:
    main()
except Exception as e:
    print(f"Unable to move files due to error : {e}")