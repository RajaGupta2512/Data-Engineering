import boto3
from datetime import datetime

s3_client = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key=''
)


bucket = 'myawsbucket12432546'
src_path = 'nextgen/lambda/source/'
tgt_path = 'nextgen/lambda/to_be_processed'
    
    
def createBatchID(use_case_id):
    
    now = datetime.now()
    
    timestamp = now.strftime("%y%m%d%H%M%S%f")[:-3]
    
    batch_id = f"{timestamp}{use_case_id}"
    
    return batch_id


def moveFilesToBeProcessed(bucket, src_prefix, target_prefix):
    
    response = s3_client.list_objects_v2(
        Bucket = bucket,
        Prefix = src_prefix
    )
    
    batch_id = createBatchID("115")
    
    if 'Contents' in response:
        for obj in response['Contents']:
              
            if len(obj['Key']) > 0:
                
                src_prefixes = obj['Key'].split("/")
                
                if src_prefixes[-1] != '':
                    
                    dest_key = f"{target_prefix}/{batch_id}/{src_prefixes[-1]}"
                        
                    s3_client.copy_object(
                        Bucket = bucket,
                        CopySource = {'Bucket': bucket, 'Key': obj['Key']},
                        Key = dest_key
                    )
                    
                    s3_client.delete_object(
                        Bucket = bucket,
                        Key = obj['Key']
                    )
                    
def main():
    moveFilesToBeProcessed(bucket, src_path, tgt_path)
 
main()