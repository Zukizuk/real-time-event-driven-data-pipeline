import json
import boto3
import os
from datetime import datetime
from urllib.parse import urlparse

s3 = boto3.client('s3')

def lambda_handler(event, context):
    order_file = event['orderFile']
    order_items_file = event['orderItemsFile']
    error_location = event['errorLocation']
    error_type = event.get('errorType', 'validation_error')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results = {
        'orders': copy_to_error(order_file, error_location, error_type, timestamp),
        'order_items': copy_to_error(order_items_file, error_location, error_type, timestamp)
    }
    
    return {
        'statusCode': 200,
        'results': results,
        'message': 'Validation failure handling completed'
    }

def copy_to_error(source_file, error_location, error_type, timestamp):
    try:
        # Parse S3 paths
        source_url = urlparse(source_file)
        source_bucket = source_url.netloc
        source_key = source_url.path.lstrip('/')
        
        # Get original filename from path
        filename = os.path.basename(source_key)
        
        # Create error destination with timestamp
        error_url = urlparse(error_location)
        error_bucket = error_url.netloc
        error_prefix = error_url.path.lstrip('/')
        error_key = f"{error_prefix}{error_type}_{timestamp}_{filename}"
        
        # Copy the file to error location
        s3.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=error_bucket,
            Key=error_key
        )
        
        return {
            'status': 'success',
            'sourceFile': source_file,
            'errorLocation': f"s3://{error_bucket}/{error_key}"
        }
    except Exception as e:
        return {
            'status': 'error',
            'sourceFile': source_file,
            'error': str(e)
        }