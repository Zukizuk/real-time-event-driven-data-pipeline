# Use official Python runtime as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the validation script
COPY validate_data.py .

# Command to run the script with a file argument
ENTRYPOINT ["python", "validate_data.py"]