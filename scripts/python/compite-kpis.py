import pandas as pd
import sys
import logging
from datetime import datetime, timezone
import boto3
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

def is_s3_path(path):
    """Check if the path is an S3 path"""
    return path.startswith('s3://')

def parse_s3_path(s3_path):
    """Extract bucket and key from S3 path"""
    path = s3_path.replace('s3://', '')
    bucket = path.split('/')[0]
    key = '/'.join(path.split('/')[1:])
    return bucket, key

def read_csv_from_s3(s3_path):
    """Read CSV file from S3"""
    bucket, key = parse_s3_path(s3_path)
    try:
        logger.debug(f"Reading data from s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
        logger.info(f"Successfully read {len(df)} records from S3")
        return df
    except Exception as e:
        logger.error(f"Failed to read from S3: {str(e)}")
        raise e

def write_csv_to_s3(df, s3_path):
    """Write dataframe to CSV in S3"""
    bucket, key = parse_s3_path(s3_path)
    try:
        logger.debug(f"Writing {len(df)} records to s3://{bucket}/{key}")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Successfully wrote data to S3")
    except Exception as e:
        logger.error(f"Failed to write to S3: {str(e)}")
        raise e

def compute_category_kpis(order_items_df):
    """Compute Category-Level KPIs."""
    df = order_items_df[['order_date', 'category', 'sale_price', 'status']]
    grouped = df.groupby(['category', 'order_date'])
    
    kpis = pd.DataFrame({
        'daily_revenue': grouped['sale_price'].sum(),
        'avg_order_value': grouped['sale_price'].mean(),
        'total_orders': grouped.size(),
        'returned_orders': grouped['status'].apply(lambda x: (x == 'returned').sum())
    }).reset_index()
    
    kpis['avg_return_rate'] = kpis['returned_orders'] / kpis['total_orders'] * 100
    kpis = kpis.drop(columns=['total_orders', 'returned_orders'])
    kpis['computed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.debug("Category-level KPIs computed")
    return kpis

def compute_order_kpis(order_items_df, orders_df):
    """Compute Order-Level KPIs."""
    orders_grouped = orders_df.groupby('order_date')
    order_kpis = pd.DataFrame({
        'total_orders': orders_grouped['order_id'].nunique(),
        'total_items_sold': orders_grouped['num_of_item'].sum(),
        'returned_orders': orders_grouped['status'].apply(lambda x: (x == 'returned').sum())
    }).reset_index()
    
    items_grouped = order_items_df.groupby('order_date')
    items_kpis = pd.DataFrame({
        'total_revenue': items_grouped['sale_price'].sum(),
        'unique_customers': items_grouped['user_id'].nunique()
    }).reset_index()
    
    kpis = order_kpis.merge(items_kpis, on='order_date', how='outer').fillna(0)
    kpis['return_rate'] = kpis['returned_orders'] / kpis['total_orders'] * 100
    kpis = kpis.drop(columns=['returned_orders'])
    kpis['computed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.debug("Order-level KPIs computed")
    return kpis

def main(order_items_file, orders_file, category_output_file, order_output_file):
    try:
        # Read transformed files
        if is_s3_path(order_items_file):
            order_items_df = read_csv_from_s3(order_items_file)
        else:
            order_items_df = pd.read_csv(order_items_file)

        if is_s3_path(orders_file):
            orders_df = read_csv_from_s3(orders_file)
        else:
            orders_df = pd.read_csv(orders_file)

        # Compute KPIs
        category_kpis = compute_category_kpis(order_items_df)
        order_kpis = compute_order_kpis(order_items_df, orders_df)

        # Save results
        if is_s3_path(category_output_file):
            write_csv_to_s3(category_kpis, category_output_file)
        else:
            category_kpis.to_csv(category_output_file, index=False)

        if is_s3_path(order_output_file):
            write_csv_to_s3(order_kpis, order_output_file)
        else:
            order_kpis.to_csv(order_output_file, index=False)

        logger.info("All KPIs saved successfully")
        print("COMPUTE_SUCCESS: ✔️ All KPIs computed and saved")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Error computing KPIs: {str(e)}")
        print(f"COMPUTE_FAILED: ❌ Error computing KPIs - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        logger.error("Please provide order_items, orders, category_output, and order_output file paths")
        print("COMPUTE_FAILED: ❌ Please provide order_items, orders, category_output, and order_output file paths")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
