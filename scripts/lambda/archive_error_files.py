import json
import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Extract the bucket and processed files from the event
    bucket = event['bucket']
    processed_files = event['processedFiles']
    
    # Define the error folder
    error_folder = 'errors/'
    
    # Move each processed file to the errors folder
    for file_key in processed_files.values():
        # Extract the file name from the S3 URI
        file_name = file_key.split('/')[-1]
        # Define the new S3 key for the errors folder
        new_key = os.path.join(error_folder, file_name)
        
        # Copy the file to the new location
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': file_key},
            Key=new_key
        )
        
        # Optionally, delete the original file
        s3.delete_object(Bucket=bucket, Key=file_key)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Files moved to errors folder successfully.')
    }
