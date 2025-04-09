import pandas as pd
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def validate_order_items(df):
    required_columns = ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'sale_price']
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        return False, f"❌ Missing columns: {missing_cols}"
    
    # Check for missing values in critical fields
    critical_fields = ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at']
    for col in critical_fields:
        if df[col].isnull().any():
            logger.error(f"Null values found in {col}")
            return False, f"❌ Null values found in {col}"
    
    # Validate status values
    valid_statuses = ['delivered', 'returned', 'shipped', 'pending']
    if not df['status'].isin(valid_statuses).all():
        logger.error("Invalid status values found")
        return False, "❌ Invalid status values found"
    
    # Validate timestamp format
    try:
        df['created_at'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'))
    except (ValueError, TypeError):
        logger.error("Invalid timestamp format in created_at")
        return False, "❌ Invalid timestamp format in created_at"
    
    # Check sale_price is numeric and non-negative
    if not pd.api.types.is_numeric_dtype(df['sale_price']) or (df['sale_price'] < 0).any():
        logger.error("Sale_price must be numeric and non-negative")
        return False, "❌ Sale_price must be numeric and non-negative"
    
    logger.info("Order items validation passed")
    return True, "✔️ Order items validation passed"

def validate_orders(df):
    required_columns = ['order_id', 'user_id', 'status', 'created_at', 'num_of_item']
    
    # Check for missing columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        return False, f"❌ Missing columns: {missing_cols}"
    
    # Check for missing values in critical fields
    critical_fields = ['order_id', 'user_id', 'status', 'created_at']
    for col in critical_fields:
        if df[col].isnull().any():
            logger.error(f"Null values found in {col}")
            return False, f"❌ Null values found in {col}"
    
    # Validate status values
    valid_statuses = ['delivered', 'returned', 'shipped', 'pending']
    if not df['status'].isin(valid_statuses).all():
        logger.error("Invalid status values found")
        return False, "❌ Invalid status values found"
    
    # Validate timestamp format
    try:
        df['created_at'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S'))
    except (ValueError, TypeError):
        logger.error("Invalid timestamp format in created_at")
        return False, "❌ Invalid timestamp format in created_at"
    
    # Check num_of_item is numeric and positive
    if not pd.api.types.is_numeric_dtype(df['num_of_item']) or (df['num_of_item'] <= 0).any():
        logger.error("num_of_item must be numeric and positive")
        return False, "❌ num_of_item must be numeric and positive"
    
    logger.info("Orders validation passed")
    return True, "✔️ Orders validation passed"

def main(file_path):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Determine file type and validate
        if 'product_id' in df.columns and 'sale_price' in df.columns:
            success, message = validate_order_items(df)
        elif 'num_of_item' in df.columns:
            success, message = validate_orders(df)
        else:
            logger.error("Unknown file format")
            print("VALIDATION_FAILED: ❌ Unknown file format")
            sys.exit(1)
        
        # Output result for Step Functions
        if success:
            logger.info(message)
            print(f"VALIDATION_SUCCESS: {message}")
            sys.exit(0)
        else:
            logger.error(message)
            print(f"VALIDATION_FAILED: {message}")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        print(f"VALIDATION_FAILED: ❌ Error processing file - {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Please provide a file path")
        print("VALIDATION_FAILED: ❌ Please provide a file path")
        sys.exit(1)
    main(sys.argv[1])