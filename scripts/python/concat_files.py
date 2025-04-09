import pandas as pd
import os

# Get all files in the data order_items directory and orders directory

data_dir = 'data'
order_items_dir = os.path.join(data_dir, 'order_items')
order_dir = os.path.join(data_dir, 'orders')

order_items_files = [f for f in os.listdir(order_items_dir) if f.endswith('.csv')]
order_files = [f for f in os.listdir(order_dir) if f.endswith('.csv')]

print(f"Found {len(order_items_files)} order items files.")
print(f"Found {len(order_files)} order files.")

order_items_paths = [os.path.join(order_items_dir, f) for f in order_items_files]
order_paths = [os.path.join(order_dir, f) for f in order_files]

# append order items files to a single file and save to output directory

output_dir = os.path.join(data_dir, 'output')
os.makedirs(output_dir, exist_ok=True)

order_items_df = pd.concat([pd.read_csv(f) for f in order_items_paths], ignore_index=True)
order_items_df.to_csv(os.path.join(output_dir, 'order_items.csv'), index=False)

print(f"Saved order items to {os.path.join(output_dir, 'order_items.csv')}")

# append orders files to a single file and save to output directory
orders_df = pd.concat([pd.read_csv(f) for f in order_paths], ignore_index=True)
orders_df.to_csv(os.path.join(output_dir, 'orders.csv'), index=False)

print(f"Saved orders to {os.path.join(output_dir, 'orders.csv')}")