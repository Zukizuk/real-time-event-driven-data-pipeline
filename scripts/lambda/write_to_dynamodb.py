import json
import boto3
import pandas as pd
import io
import uuid
from decimal import Decimal

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

def parse_s3_path(s3_path):
    """Extract bucket and key from S3 path"""
    path = s3_path.replace('s3://', '')
    bucket = path.split('/')[0]
    key = '/'.join(path.split('/')[1:])
    return bucket, key

def read_csv_from_s3(s3_path):
    """Read CSV file from S3"""
    bucket, key = parse_s3_path(s3_path)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    return df

def write_to_dynamodb(df, table_name):
    """Write dataframe to DynamoDB table, converting floats to Decimal."""
    table = dynamodb.Table(table_name)
    records = df.to_dict('records')

    with table.batch_writer() as batch:
        for record in records:
            # 1) Convert any floats (including numpy.float64) to Decimal
            for k, v in record.items():
                # catch both built‑in floats and numpy floats
                if isinstance(v, float) or (hasattr(v, "dtype") and v.dtype.kind == "f"):
                    # Decimal(str()) is safest to avoid binary floating‑point artifacts
                    record[k] = Decimal(str(v))

            # 2) Now push the fully Decimal‑ized item
            batch.put_item(Item=record)

    return len(records)

def lambda_handler(event, context):
    try:
        # Extract file paths and table names from event
        category_kpi_file = event['category_kpi_file']
        order_kpi_file = event['order_kpi_file']
        category_table = event['category_table']
        order_table = event['order_table']
        
        # Read CSV files from S3
        category_kpis_df = read_csv_from_s3(category_kpi_file)
        order_kpis_df = read_csv_from_s3(order_kpi_file)
        
        # Write to DynamoDB tables
        category_count = write_to_dynamodb(category_kpis_df, category_table)
        order_count = write_to_dynamodb(order_kpis_df, order_table)

        # Send tasktoken
        client = boto3.client('stepfunctions')
        client.send_task_success(
            taskToken=event['taskToken'],
            output=json.dumps({"status": "completed"})
        )
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully imported KPIs to DynamoDB',
                'category_records': category_count,
                'order_records': order_count
            })
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e