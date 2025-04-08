import pandas as pd
import json
import logging
from datetime import datetime, timedelta
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self):
        self.transformation_results = {
            "success": True,
            "errors": [],
            "kpis_generated": {
                "category_level": 0,
                "order_level": 0
            }
        }
        
    def load_data(self, orders_path, order_items_path, products_path):
        """Load data from CSV files"""
        try:
            orders_df = pd.read_csv(orders_path)
            order_items_df = pd.read_csv(order_items_path)
            products_df = pd.read_csv(products_path)
            
            logger.info(f"Loaded {len(orders_df)} orders, {len(order_items_df)} order items, and {len(products_df)} products")
            return orders_df, order_items_df, products_df
            
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error loading data: {str(e)}")
            logger.error(f"Error loading data: {str(e)}")
            return None, None, None
    
    def preprocess_data(self, orders_df, order_items_df, products_df):
        """Preprocess and merge data for KPI calculation"""
        try:
            # Convert date strings to datetime objects
            for df in [orders_df, order_items_df]:
                for col in ['created_at', 'shipped_at', 'delivered_at', 'returned_at']:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Extract date from datetime for grouping
            orders_df['order_date'] = orders_df['created_at'].dt.date
            order_items_df['order_date'] = order_items_df['created_at'].dt.date
            
            # Merge order items with product information
            enriched_items = pd.merge(
                order_items_df,
                products_df,
                how='left',
                left_on='product_id',
                right_on='id',
                suffixes=('', '_product')
            )
            
            # Merge with orders for additional information
            enriched_data = pd.merge(
                enriched_items,
                orders_df,
                how='left',
                on='order_id',
                suffixes=('_item', '')
            )
            
            logger.info(f"Preprocessed data with {len(enriched_data)} rows")
            return enriched_data
            
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error preprocessing data: {str(e)}")
            logger.error(f"Error preprocessing data: {str(e)}")
            return None
    
    def extract_category_kpis(self, enriched_data):
        """
        Extract category-level KPIs:
        - category
        - order_date
        - daily_revenue
        - avg_order_value
        - avg_return_rate
        """
        try:
            # This is just the structure setup - actual computation will be implemented later
            category_kpis = []
            # We'll group by category and date to calculate these metrics
            
            # For now, just create the structure
            unique_categories = enriched_data['category'].unique()
            unique_dates = enriched_data['order_date'].unique()
            
            logger.info(f"Prepared structure for {len(unique_categories)} categories × {len(unique_dates)} dates")
            return category_kpis
            
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error extracting category KPIs: {str(e)}")
            logger.error(f"Error extracting category KPIs: {str(e)}")
            return []
    
    def extract_order_kpis(self, enriched_data):
        """
        Extract order-level KPIs:
        - order_date
        - total_orders
        - total_revenue
        - total_items_sold
        - return_rate
        - unique_customers
        """
        try:
            # This is just the structure setup - actual computation will be implemented later
            order_kpis = []
            # We'll group by date to calculate these metrics
            
            # For now, just create the structure
            unique_dates = enriched_data['order_date'].unique()
            
            logger.info(f"Prepared structure for {len(unique_dates)} dates of order-level KPIs")
            return order_kpis
            
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error extracting order KPIs: {str(e)}")
            logger.error(f"Error extracting order KPIs: {str(e)}")
            return []
    
    def prepare_dynamo_items(self, category_kpis, order_kpis):
        """
        Prepare items for DynamoDB in the correct format:
        - Convert dates to ISO strings
        - Ensure numeric values are the right type
        """
        try:
            # This is just placeholder code - will be implemented later
            category_items = []
            order_items = []
            
            # For category KPIs - format for DynamoDB
            # For order KPIs - format for DynamoDB
            
            logger.info(f"Prepared {len(category_items)} category items and {len(order_items)} order items for DynamoDB")
            return category_items, order_items
            
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error preparing DynamoDB items: {str(e)}")
            logger.error(f"Error preparing DynamoDB items: {str(e)}")
            return [], []
    
    def transform_data(self, orders_path, order_items_path, products_path, output_dir="./output"):
        """Main method to transform data and generate KPIs"""
        logger.info("Starting data transformation...")
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Load data
        orders_df, order_items_df, products_df = self.load_data(orders_path, order_items_path, products_path)
        if orders_df is None or order_items_df is None or products_df is None:
            return self.transformation_results
        
        # Preprocess data
        enriched_data = self.preprocess_data(orders_df, order_items_df, products_df)
        if enriched_data is None:
            return self.transformation_results
        
        # Extract KPIs (not computing yet, just setting up structure)
        category_kpis = self.extract_category_kpis(enriched_data)
        order_kpis = self.extract_order_kpis(enriched_data)
        
        # Prepare for DynamoDB
        category_items, order_items = self.prepare_dynamo_items(category_kpis, order_kpis)
        
        # Save results for review
        try:
            with open(f"{output_dir}/category_items.json", "w") as f:
                json.dump(category_items, f, indent=2)
                
            with open(f"{output_dir}/order_items.json", "w") as f:
                json.dump(order_items, f, indent=2)
                
            logger.info(f"Results saved to {output_dir}")
        except Exception as e:
            self.transformation_results["success"] = False
            self.transformation_results["errors"].append(f"Error saving results: {str(e)}")
            logger.error(f"Error saving results: {str(e)}")
        
        return self.transformation_results

# Example usage
if __name__ == "__main__":
    transformer = DataTransformer()
    results = transformer.transform_data(
        "orders.csv",
        "order_items.csv",
        "products.csv"
    )
    
    # Output results to a file
    with open("transformation_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"Transformation {'completed successfully' if results['success'] else 'failed'}")
    if not results["success"]:
        print(f"Found {len(results['errors'])} errors.")