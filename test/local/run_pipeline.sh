#!/bin/bash
cd "$(dirname "$0")"

# Validate
echo "Validating files..."
docker run --rm -v $(pwd)/data:/app/data validate-data:latest /app/data/order_items.csv || exit 1
docker run --rm -v $(pwd)/data:/app/data validate-data:latest /app/data/orders.csv || exit 1

# Transform
echo "Transforming files..."
docker run --rm -v $(pwd)/data:/app/data transform-data:latest /app/data/order_items.csv /app/data/products.csv /app/data/output/order_items_transformed.csv || exit 1
docker run --rm -v $(pwd)/data:/app/data transform-data:latest /app/data/orders.csv none /app/data/output/orders_transformed.csv || exit 1
docker run --rm -v $(pwd)/data:/app/data transform-data:latest /app/data/products.csv none /app/data/output/products_transformed.csv || exit 1

# Compute KPIs
echo "Computing KPIs..."
docker run --rm -v $(pwd)/data:/app/data compute-kpis:latest /app/data/output/order_items_transformed.csv /app/data/output/orders_transformed.csv /app/data/output/category_kpis.csv /app/data/output/order_kpis.csv || exit 1

echo "Pipeline completed successfully!"