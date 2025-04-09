import pandas as pd
import sys
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def transform_order_items(df, products_df):
    # Standardize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    products_df.columns = [col.lower().replace(' ', '_') for col in products_df.columns]
    
    # Log input columns for debugging
    logger.info(f"Order items columns: {list(df.columns)}")
    logger.info(f"Products columns: {list(products_df.columns)}")
    
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
    
    df = df.merge(products_df[['id', 'category']], 
                  left_on='product_id', 
                  right_on='id', 
                  how='left')
    
    # Log post-merge columns
    logger.info(f"Post-merge columns: {list(df.columns)}")
    
    # Drop 'id' only if it exists
    if 'id' in df.columns:
        df = df.drop(columns=['id'])
    
    # Keep only relevant columns for KPIs
    keep_cols = ['order_id', 'user_id', 'product_id', 'status', 'created_at', 'order_date', 
                 'sale_price', 'category', 'returned_at']
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
     # df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info("Order items transformation completed")
    return df, "✔️ Order items transformation completed"

def transform_orders(df):
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
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
     # df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info("Orders transformation completed")
    return df, "✔️ Orders transformation completed"

def transform_products(df):
    # Standardize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    # Convert prices to float
    df['cost'] = df['cost'].astype(float)
    df['retail_price'] = df['retail_price'].astype(float)
    
    # Keep only relevant columns for joining (id and category)
    keep_cols = ['id', 'category']
    df = df[keep_cols]
    
    # Add transformed_at timestamp (timezone-aware UTC)
     # df.loc[:, 'transformed_at'] = datetime.now(timezone.utc).isoformat()
    
    logger.info("Products transformation completed")
    return df, "✔️ Products transformation completed"

def main(input_file, products_file, output_file):
    try:
        # Read the input CSV and products CSV
        df = pd.read_csv(input_file)
        products_df = pd.read_csv(products_file) if products_file else None
        
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
            logger.error("Unknown file format")
            print("TRANSFORM_FAILED: ❌ Unknown file format")
            sys.exit(1)
        
        # Save transformed data
        transformed_df.to_csv(output_file, index=False)
        
        # Output result for Step Functions
        logger.info(message)
        print(f"TRANSFORM_SUCCESS: {message}")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Error transforming file: {str(e)}")
        print(f"TRANSFORM_FAILED: ❌ Error transforming file - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("Please provide input file, products file (or 'none' if not needed), and output file paths")
        print("TRANSFORM_FAILED: ❌ Please provide input file, products file (or 'none' if not needed), and output file paths")
        sys.exit(1)
    input_file, products_file, output_file = sys.argv[1], sys.argv[2], sys.argv[3]
    main(input_file, products_file if products_file != 'none' else None, output_file)