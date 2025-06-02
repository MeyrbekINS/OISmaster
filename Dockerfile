# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container at /app
COPY OIS_Fetcher.py .

# Set environment variables
# --- IMPORTANT: Update DYNAMODB_TABLE_NAME if you created a new one ---
ENV DYNAMODB_TABLE_NAME="OISRATES"
ENV METRIC_ID_OIS="CALCULATED_OIS_1M_RATE"
ENV METRIC_ID_IMPLIED_FF="IMPLIED_FF_RATE"
ENV AWS_REGION="eu-north-1"

# Run OIS_Fetcher.py when the container launches
CMD ["python", "OIS_Fetcher.py"]
