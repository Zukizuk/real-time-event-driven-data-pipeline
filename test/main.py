# This script uploads files from local directories to an S3 bucket and creates a manifest file to test the event driven pipeline
import boto3
import os
import json
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# Configuration
BUCKET_NAME = 'your-bucket'  # Replace with your bucket name
LOCAL_BASE_PATH = 'data'  # Local folder containing orders/ and order_items/
DATE = datetime.now().strftime('%Y%m%d')  # e.g., '20250409'
S3_BASE_PATH = f'data/{DATE}/'

# Function to upload a file to S3
def upload_file_to_s3(local_path, s3_key):
    s3.upload_file(
        Filename=local_path,
        Bucket=BUCKET_NAME,
        Key=s3_key
    )
    print(f'Uploaded {local_path} to s3://{BUCKET_NAME}/{s3_key}')

# Function to upload the manifest
def upload_manifest(orders_files, order_items_files):
    manifest = {
        'date': DATE,
        'files': {
            'orders': orders_files,
            'order_items': order_items_files
        }
    }
    manifest_key = f'{S3_BASE_PATH}manifest_{DATE}.json'
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    upload_file_to_s3('manifest.json', manifest_key)
    os.remove('manifest.json')  # Clean up local temp file

# Main execution
if __name__ == '__main__':
    # Local folder paths
    orders_dir = os.path.join(LOCAL_BASE_PATH, 'orders')
    order_items_dir = os.path.join(LOCAL_BASE_PATH, 'order_items')

    # Lists to track files for the manifest
    orders_files = []
    order_items_files = []

    # Upload orders files
    if os.path.exists(orders_dir):
        for filename in os.listdir(orders_dir):
            local_path = os.path.join(orders_dir, filename)
            if os.path.isfile(local_path) and 'Zone.Identifier' not in filename:
                s3_key = f'{S3_BASE_PATH}orders/{filename}'
                upload_file_to_s3(local_path, s3_key)
                orders_files.append(filename)
    else:
        print(f'Warning: {orders_dir} not found locally')

    # Upload order_items files
    if os.path.exists(order_items_dir):
        for filename in os.listdir(order_items_dir):
            local_path = os.path.join(order_items_dir, filename)
            if os.path.isfile(local_path) and 'Zone.Identifier' not in filename:
                s3_key = f'{S3_BASE_PATH}order_items/{filename}'
                upload_file_to_s3(local_path, s3_key)
                order_items_files.append(filename)
    else:
        print(f'Warning: {order_items_dir} not found locally')

    # Upload manifest last (triggers Lambda)
    if orders_files or order_items_files:
        upload_manifest(orders_files, order_items_files)
    else:
        print('No files found to upload, skipping manifest')
