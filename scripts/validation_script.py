import pandas as pd
import os
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.validation_results = {
            "is_valid": True,
            "errors": [],
            "file_stats": {}
        }
    
    def validate_file_exists(self, file_path):
        """Check if file exists"""
        if not os.path.exists(file_path):
            self.validation_results["is_valid"] = False
            self.validation_results["errors"].append(f"File not found: {file_path}")
            return False
        return True
    
    def validate_csv_format(self, file_path):
        """Check if file is a valid CSV"""
        try:
            df = pd.read_csv(file_path)
            self.validation_results["file_stats"][os.path.basename(file_path)] = {
                "row_count": len(df),
                "column_count": len(df.columns)
            }
            return df
        except Exception as e:
            self.validation_results["is_valid"] = False
            self.validation_results["errors"].append(f"Invalid CSV format in {os.path.basename(file_path)}: {str(e)}")
            return None
    
    def validate_required_columns(self, df, file_name, required_columns):
        """Check if all required columns are present"""
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.validation_results["is_valid"] = False
            self.validation_results["errors"].append(
                f"Missing required columns in {file_name}: {', '.join(missing_columns)}"
            )
            return False
        return True
    
    def validate_no_duplicate_keys(self, df, file_name, key_column):
        """Check for duplicate primary keys"""
        if df[key_column].duplicated().any():
            duplicate_keys = df[df[key_column].duplicated()][key_column].tolist()
            self.validation_results["is_valid"] = False
            self.validation_results["errors"].append(
                f"Duplicate primary keys found in {file_name}: {duplicate_keys[:5]}{'...' if len(duplicate_keys) > 5 else ''}"
            )
            return False
        return True
    
    def validate_datetime_format(self, df, file_name, datetime_columns):
        """Validate datetime formats"""
        for column in datetime_columns:
            if column in df.columns:
                try:
                    # Convert non-empty values to datetime
                    mask = df[column].notna()
                    if mask.any():
                        pd.to_datetime(df.loc[mask, column])
                except Exception as e:
                    self.validation_results["is_valid"] = False
                    self.validation_results["errors"].append(
                        f"Invalid datetime format in column {column} of {file_name}: {str(e)}"
                    )
                    return False
        return True
    
    def validate_foreign_keys(self, df, file_name, fk_column, reference_df, ref_column):
        """Validate foreign key relationships"""
        if fk_column in df.columns and ref_column in reference_df.columns:
            foreign_keys = set(df[fk_column].dropna().unique())
            reference_keys = set(reference_df[ref_column].unique())
            invalid_keys = foreign_keys - reference_keys
            
            if invalid_keys:
                self.validation_results["is_valid"] = False
                self.validation_results["errors"].append(
                    f"Invalid foreign keys in {file_name}.{fk_column}: {list(invalid_keys)[:5]}{'...' if len(invalid_keys) > 5 else ''}"
                )
                return False
        return True
    
    def validate_numeric_fields(self, df, file_name, numeric_columns):
        """Validate numeric fields"""
        for column in numeric_columns:
            if column in df.columns:
                non_numeric = df[df[column].notna() & ~df[column].astype(str).str.replace('.', '', 1).str.isdigit()]
                if not non_numeric.empty:
                    self.validation_results["is_valid"] = False
                    self.validation_results["errors"].append(
                        f"Non-numeric values found in {file_name}.{column}: {non_numeric[column].tolist()[:5]}"
                    )
                    return False
        return True
    
    def validate_status_values(self, df, file_name, status_column, valid_statuses):
        """Validate status values are in allowed list"""
        if status_column in df.columns:
            invalid_statuses = df[~df[status_column].isin(valid_statuses)][status_column].unique()
            if len(invalid_statuses) > 0:
                self.validation_results["is_valid"] = False
                self.validation_results["errors"].append(
                    f"Invalid status values in {file_name}.{status_column}: {invalid_statuses.tolist()}"
                )
                return False
        return True
    
    def validate_orders(self, orders_path):
        """Validate orders.csv file"""
        if not self.validate_file_exists(orders_path):
            return None
        
        df = self.validate_csv_format(orders_path)
        if df is None:
            return None
            
        file_name = os.path.basename(orders_path)
        
        # Check required columns
        required_columns = ['order_id', 'user_id', 'status', 'created_at', 'shipped_at', 'delivered_at', 'num_of_item']
        if not self.validate_required_columns(df, file_name, required_columns):
            return None
            
        # Check for duplicate order_ids
        if not self.validate_no_duplicate_keys(df, file_name, 'order_id'):
            return None
            
        # Validate datetime columns
        datetime_columns = ['created_at', 'returned_at', 'shipped_at', 'delivered_at']
        if not self.validate_datetime_format(df, file_name, datetime_columns):
            return None
            
        # Validate status values
        valid_statuses = ['delivered', 'returned', 'shipped', 'pending']
        if not self.validate_status_values(df, file_name, 'status', valid_statuses):
            return None
            
        # Validate numeric fields
        numeric_columns = ['num_of_item']
        if not self.validate_numeric_fields(df, file_name, numeric_columns):
            return None
            
        return df
        
    def validate_order_items(self, order_items_path, orders_df=None):
        """Validate order_items.csv file"""
        if not self.validate_file_exists(order_items_path):
            return None
            
        df = self.validate_csv_format(order_items_path)
        if df is None:
            return None
            
        file_name = os.path.basename(order_items_path)
        
        # Check required columns
        required_columns = ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'sale_price']
        if not self.validate_required_columns(df, file_name, required_columns):
            return None
            
        # Check for duplicate IDs
        if not self.validate_no_duplicate_keys(df, file_name, 'id'):
            return None
            
        # Validate datetime columns
        datetime_columns = ['created_at', 'shipped_at', 'delivered_at', 'returned_at']
        if not self.validate_datetime_format(df, file_name, datetime_columns):
            return None
            
        # Validate numeric fields
        numeric_columns = ['sale_price']
        if not self.validate_numeric_fields(df, file_name, numeric_columns):
            return None
            
        # Validate status values
        valid_statuses = ['delivered', 'returned', 'shipped', 'pending']
        if not self.validate_status_values(df, file_name, 'status', valid_statuses):
            return None
            
        # Validate foreign keys if orders_df is provided
        if orders_df is not None:
            if not self.validate_foreign_keys(df, file_name, 'order_id', orders_df, 'order_id'):
                return None
                
        return df
        
    def validate_products(self, products_path):
        """Validate products.csv file"""
        if not self.validate_file_exists(products_path):
            return None
            
        df = self.validate_csv_format(products_path)
        if df is None:
            return None
            
        file_name = os.path.basename(products_path)
        
        # Check required columns
        required_columns = ['id', 'category', 'cost', 'retail_price']
        if not self.validate_required_columns(df, file_name, required_columns):
            return None
            
        # Check for duplicate IDs
        if not self.validate_no_duplicate_keys(df, file_name, 'id'):
            return None
            
        # Validate numeric fields
        numeric_columns = ['cost', 'retail_price']
        if not self.validate_numeric_fields(df, file_name, numeric_columns):
            return None
            
        return df
        
    def validate_dataset(self, orders_path, order_items_path, products_path):
        """Validate all dataset files and their relationships"""
        logger.info("Starting data validation...")
        
        # Validate individual files
        orders_df = self.validate_orders(orders_path)
        products_df = self.validate_products(products_path)
        
        # Validate order_items with reference to orders
        order_items_df = self.validate_order_items(order_items_path, orders_df)
        
        # Validate relationships between order_items and products
        if order_items_df is not None and products_df is not None:
            self.validate_foreign_keys(
                order_items_df, 
                os.path.basename(order_items_path), 
                'product_id', 
                products_df, 
                'id'
            )
        
        # Log validation results
        if self.validation_results["is_valid"]:
            logger.info("Validation successful!")
        else:
            logger.error(f"Validation failed with {len(self.validation_results['errors'])} errors")
            for error in self.validation_results["errors"]:
                logger.error(f"- {error}")
        
        return self.validation_results

# Example usage
if __name__ == "__main__":
    validator = DataValidator()
    results = validator.validate_dataset(
        "orders.csv",
        "order_items.csv",
        "products.csv"
    )
    
    # Output results to a file
    with open("validation_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"Validation {'passed' if results['is_valid'] else 'failed'}")
    if not results["is_valid"]:
        print(f"Found {len(results['errors'])} errors.")