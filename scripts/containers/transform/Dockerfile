# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the transformation script
COPY transform_data.py .

# Command to run the script with input and output file arguments
ENTRYPOINT ["python", "transform_data.py"]