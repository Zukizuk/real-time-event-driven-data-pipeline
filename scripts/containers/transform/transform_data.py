import pandas as pd
import sys
import logging
import os
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
        logger.info(f"Reading data from s3://{bucket}/{key}")
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
        logger.info(f"Writing {len(df)} records to s3://{bucket}/{key}")
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        logger.info(f"Successfully wrote data to S3")
        return True
    except Exception as e:
        logger.error(f"Failed to write to S3: {str(e)}")
        raise e

def transform_order_items(df, products_df):
    """Transform order items data"""
    logger.info(f"Starting order items transformation process")
    
    # Standardize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    products_df.columns = [col.lower().replace(' ', '_') for col in products_df.columns]
    
    # Convert timestamps to datetime
    time_cols = ['created_at', 'shipped_at', 'delivered_at', 'returned_at']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert sale_price to float
    df['sale_price'] = df['sale_price'].astype(float)
    
    # Add order_date for KPI grouping
    df['order_date'] = df['created_at'].dt.date
    
    # Join with products to get category
    if 'id' not in products_df.columns:
        logger.error("Products dataframe missing 'id' column")
        raise ValueError("Products dataframe missing 'id' column")
    
    logger.info("Joining order items with product data")
    df = df.merge(products_df[['id', 'category']], 
                  left_on='product_id', 
                  right_on='id', 
                  how='left')
    
    # Handle column renaming due to merge
    if 'id_x' in df.columns and 'id_y' in df.columns:
        df = df.rename(columns={'id_x': 'id'}).drop(columns=['id_y'])
    
    # Keep only relevant columns for KPIs
    keep_cols = ['order_id', 'user_id', 'product_id', 'status', 'created_at', 'order_date', 
                 'sale_price', 'category', 'returned_at']
    # Only keep columns that exist in the dataframe
    keep_cols = [col for col in keep_cols if col in df.columns]
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
    df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"Order items transformation completed successfully: {len(df)} records processed")
    return df, "✔️ Order items transformation completed"

def transform_orders(df):
    """Transform orders data"""
    logger.info(f"Starting orders transformation process")
    
    # Standardize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Convert timestamps to datetime
    time_cols = ['created_at', 'shipped_at', 'delivered_at', 'returned_at']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert num_of_item to integer
    df['num_of_item'] = df['num_of_item'].astype(int)
    
    # Add order_date for KPI grouping
    df['order_date'] = df['created_at'].dt.date
    
    # Keep only relevant columns for KPIs
    keep_cols = ['order_id', 'user_id', 'status', 'created_at', 'order_date', 'num_of_item', 'returned_at']
    # Only keep columns that exist in the dataframe
    keep_cols = [col for col in keep_cols if col in df.columns]
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
    df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"Orders transformation completed successfully: {len(df)} records processed")
    return df, "✔️ Orders transformation completed"

def transform_products(df):
    """Transform products data"""
    logger.info(f"Starting products transformation process")
    
    # Standardize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Convert prices to float
    df['cost'] = df['cost'].astype(float)
    df['retail_price'] = df['retail_price'].astype(float)
    
    # Keep only relevant columns for joining (id and category)
    keep_cols = ['id', 'category']
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
    df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info(f"Products transformation completed successfully: {len(df)} records processed")
    return df, "✔️ Products transformation completed"

def main(input_file, products_file, output_file):
    try:
        logger.info(f"Starting transformation process")
        logger.info(f"Input file: {input_file}")
        logger.info(f"Products file: {products_file}")
        logger.info(f"Output file: {output_file}")
        
        # Read CSV file(s) based on path type
        if is_s3_path(input_file):
            df = read_csv_from_s3(input_file)
        else:
            logger.info(f"Reading data from local file: {input_file}")
            df = pd.read_csv(input_file)
            logger.info(f"Successfully read {len(df)} records from local file")
        
        # Read products file if provided
        products_df = None
        if products_file:
            if is_s3_path(products_file):
                products_df = read_csv_from_s3(products_file)
            else:
                logger.info(f"Reading products data from local file: {products_file}")
                products_df = pd.read_csv(products_file)
                logger.info(f"Successfully read {len(products_df)} records from local file")
        
        # Transform based on file type
        if 'product_id' in df.columns and 'sale_price' in df.columns:
            if products_df is None:
                logger.error("Products file required for order_items transformation")
                print("TRANSFORM_FAILED: ❌ Products file required for order_items transformation")
                sys.exit(1)
            transformed_df, message = transform_order_items(df, products_df)
        elif 'num_of_item' in df.columns:
            transformed_df, message = transform_orders(df)
        elif 'sku' in df.columns:
            transformed_df, message = transform_products(df)
        else:
            logger.error("Unable to determine file type from columns")
            print("TRANSFORM_FAILED: ❌ Unknown file format")
            sys.exit(1)
        
        # Save transformed data
        if is_s3_path(output_file):
            write_csv_to_s3(transformed_df, output_file)
        else:
            logger.info(f"Writing {len(transformed_df)} records to local file: {output_file}")
            transformed_df.to_csv(output_file, index=False)
            logger.info("Successfully wrote data to local file")
        
        # Output result for Step Functions
        logger.info(f"Transformation completed successfully: {message}")
        print(f"TRANSFORM_SUCCESS: {message}")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Error in transformation process: {str(e)}")
        print(f"TRANSFORM_FAILED: ❌ Error transforming file - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Get file paths from environment variables instead of sys.argv
    input_file = os.environ.get("INPUT_FILE")
    products_file = os.environ.get("PRODUCTS_FILE")
    output_file = os.environ.get("OUTPUT_FILE")
    
    if not input_file or not output_file:  # products_file can be optional
        logger.error("Required environment variables missing (INPUT_FILE and OUTPUT_FILE are mandatory)")
        print("TRANSFORM_FAILED: ❌ Please provide INPUT_FILE and OUTPUT_FILE environment variables")
        sys.exit(1)
    
    # Handle 'none' case for products_file
    products_file = None if products_file == 'none' else products_file
    
    main(input_file, products_file, output_file)