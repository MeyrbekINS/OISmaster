import cloudscraper
import boto3
import os
import time
from datetime import datetime, timezone
import requests

# Configuration
API_URL = "https://api.investing.com/api/financialdata/1199741/historical/chart/?interval=P1D&pointscount=160"
# Assuming the table name is OISRATES as per your Dockerfile log
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'OISRATES')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')

# Metric IDs
METRIC_ID_OIS = os.environ.get('METRIC_ID_OIS', 'CALCULATED_OIS_1M_RATE')
METRIC_ID_IMPLIED_FF = os.environ.get('METRIC_ID_IMPLIED_FF', 'IMPLIED_FF_RATE') # New Metric ID

dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

def fetch_ois_data():
    """Fetches raw Fed Funds futures data from Investing.com API using cloudscraper."""
    print(f"Fetching Fed Funds futures data from: {API_URL} using cloudscraper")
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(API_URL)
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and 'data' in data:
            actual_data_points = data.get('data', [])
        elif isinstance(data, list):
            actual_data_points = data
        else:
            print("Unexpected data format from API.")
            return None
        print(f"Successfully fetched data. Number of points: {len(actual_data_points)}")
        return actual_data_points
    except requests.exceptions.RequestException as e: # cloudscraper uses requests' exceptions
        print(f"Error fetching data from API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
    except ValueError as e:
        print(f"Error decoding JSON response: {e}")
        print(f"Response text: {response.text if 'response' in locals() else 'No response object'}")
    return None

def calculate_and_store_rates(raw_data): # Renamed function for clarity
    """
    Calculates 1-month OIS rate proxy and Implied FF Rate from Fed Funds futures data
    and stores them in DynamoDB. Processes the last 3 data points.
    """
    if not raw_data or len(raw_data) == 0:
        print("No raw data to process.")
        return

    points_to_process = raw_data[-3:]
    print(f"Processing the last {len(points_to_process)} data points for OIS and Implied FF Rate calculation.")

    items_to_put = []
    for point in points_to_process:
        try:
            timestamp_ms = int(point[0])
            close_price = float(point[1])

            # Step 1: Calculate the implied annual Fed Funds rate (decimal)
            implied_annual_ff_rate_decimal = (100.0 - close_price) / 100.0
            implied_ff_rate_to_store_percent = implied_annual_ff_rate_decimal * 100.0

            # Prepare item for Implied FF Rate
            item_ff = {
                'metricId': {'S': METRIC_ID_IMPLIED_FF},
                'timestamp': {'N': str(timestamp_ms)},
                'value': {'N': f"{implied_ff_rate_to_store_percent:.4f}"}
            }
            items_to_put.append({'PutRequest': {'Item': item_ff}})

            # Step 2: Calculate OIS Rate
            daily_rate_decimal = implied_annual_ff_rate_decimal / 360.0
            n_days = 30.0
            if 1 + daily_rate_decimal <= 0:
                print(f"Skipping OIS calculation for point {point} due to invalid daily_rate_decimal: {daily_rate_decimal}")
                ois_rate_to_store_percent = None # Or some indicator it couldn't be calculated
            else:
                compounded_rate_decimal = ((1 + daily_rate_decimal)**n_days - 1) * (360.0 / n_days)
                ois_rate_to_store_percent = compounded_rate_decimal * 100.0

            # Prepare item for OIS Rate (only if calculable)
            if ois_rate_to_store_percent is not None:
                item_ois = {
                    'metricId': {'S': METRIC_ID_OIS},
                    'timestamp': {'N': str(timestamp_ms)},
                    'value': {'N': f"{ois_rate_to_store_percent:.4f}"}
                }
                items_to_put.append({'PutRequest': {'Item': item_ois}})
            
            try:
                dt_object = datetime.fromtimestamp(timestamp_ms / 1000.0, datetime.UTC)
            except AttributeError:
                dt_object = datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc)

            print(f"Data for Timestamp: {dt_object.strftime('%Y-%m-%d %H:%M:%S')} UTC, Futures Close: {close_price}")
            print(f"  Implied FF Rate (ann.): {implied_ff_rate_to_store_percent:.4f}%")
            if ois_rate_to_store_percent is not None:
                print(f"  Calculated OIS (1M ann.): {ois_rate_to_store_percent:.4f}%")

        except (IndexError, ValueError, TypeError) as e:
            print(f"Error processing data point {point}: {e}")
            continue

    if items_to_put:
        try:
            # For BatchWriteItem, max 25 items per request. Here we have at most 2*3=6 items.
            # Chunks of 25 if we were processing more items.
            # For simplicity with few items, individual PutItem is also fine as before.
            # Let's stick to individual PutItem to avoid changing too much from working code.
            for req in items_to_put:
                 dynamodb_client.put_item(
                    TableName=DYNAMODB_TABLE_NAME,
                    Item=req['PutRequest']['Item']
                )
            print(f"Successfully stored/updated {len(items_to_put)} data points in DynamoDB.")
        except Exception as e:
            print(f"Error storing data in DynamoDB: {e}")
    else:
        print("No valid data points were prepared for storage.")

if __name__ == "__main__":
    print("Starting OIS & Implied FF Rate Fetcher Script...")
    raw_ff_futures_data = fetch_ois_data()
    if raw_ff_futures_data:
        calculate_and_store_rates(raw_ff_futures_data) # Use the renamed function
    print("OIS & Implied FF Rate Fetcher Script finished.")
