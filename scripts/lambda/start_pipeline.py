import json
import boto3
import csv
from io import StringIO
import datetime

# Initialize S3 client and Step Functions client
s3 = boto3.client('s3')
step_functions = boto3.client('stepfunctions')

def lambda_handler(event, context):
    # Extract bucket and manifest key from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    manifest_key = event['Records'][0]['s3']['object']['key']
    
    # Parse date and base path from manifest key (e.g., 'data/20250409/manifest_20250409.json')
    date = manifest_key.split('/')[1]  # e.g., '20250409'
    base_path = f'data/{date}/'
    
    # Dictionary to store processed file paths
    processed_files = {}
    
    # Fetch and read the manifest file
    manifest_obj = s3.get_object(Bucket=bucket, Key=manifest_key)
    manifest_data = json.loads(manifest_obj['Body'].read().decode('utf-8'))
    
    # Process both orders and order_items
    for file_type in ['orders', 'order_items']:
        # Get list of file parts from manifest
        file_parts = manifest_data['files'].get(file_type, [])
        if not file_parts:
            continue  # Skip if no files for this type
        print(f"📄 Processing {file_type} for {date}")

        # Prepare to merge CSV content
        merged_content = StringIO()
        csv_writer = csv.writer(merged_content)
        header_written = False
        
        # Fetch and merge each part
        for part_file in file_parts:
            part_key = f'{base_path}{file_type}/{part_file}'
            print(f"📄 Merging {part_key}")
            try:
                part_obj = s3.get_object(Bucket=bucket, Key=part_key)
                part_content = part_obj['Body'].read().decode('utf-8').splitlines()
            except s3.exceptions.NoSuchKey:
                print(f"❌ File not found: {part_key}")
                continue
            
            # Read CSV rows
            csv_reader = csv.reader(part_content)
            header = next(csv_reader)  # First row is header
            
            if not header_written:
                # Write header from first file only
                csv_writer.writerow(header)
                header_written = True
            
            # Write all data rows (skip header if not first file)
            for row in csv_reader:
                csv_writer.writerow(row)
        
        # Generate merged file key (e.g., 'processed/20250409/orders_merged.csv')
        merged_key = f'processed/{date}/{file_type}_merged.csv'
        
        # Upload merged content to S3
        s3.put_object(
            Bucket=bucket,
            Key=merged_key,
            Body=merged_content.getvalue()
        )
        
        # Store S3 URI of processed file
        processed_files[file_type] = f's3://{bucket}/{merged_key}'
    
    # Start Step Function with the processed file paths
    step_function_input = {
        'date': date,
        'bucket': bucket,
        'processedFiles': processed_files
    }
    
    # Replace with your actual state machine ARN
    state_machine_arn = 'arn:aws:states:eu-west-1:123456789:stateMachine:EcommerceValidationPipeline'
    
    # Execute Step Function
    response = step_functions.start_execution(
        stateMachineArn=state_machine_arn,
        name=f'ProcessingRun-{date}-{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}',
        input=json.dumps(step_function_input)
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Merged files for {date} successfully placed in processed/',
            'stepFunctionExecution': response['executionArn'],
            'processedFiles': processed_files
        })
    }