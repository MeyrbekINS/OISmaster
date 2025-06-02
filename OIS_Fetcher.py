import cloudscraper
import boto3
import os
import time
from datetime import datetime, timezone # Added timezone

# Configuration
API_URL = "https://api.investing.com/api/financialdata/1199741/historical/chart/?interval=P1D&pointscount=160"
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'OISRATES')
METRIC_ID = os.environ.get('METRIC_ID', 'CALCULATED_OIS_1M_RATE') # Renamed for clarity (1M for 1-month)
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')

dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

def fetch_ois_data():
    """Fetches raw Fed Funds futures data from Investing.com API using cloudscraper."""
    print(f"Fetching Fed Funds futures data from: {API_URL} using cloudscraper")
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(API_URL)
        response.raise_for_status()
        data = response.json()
        # Investing.com API for historical data returns a list of lists
        # [[timestamp_ms, close, open, high, low, volume_str, pretty_date], ...]
        # Validate if 'data' key exists or if response.json() is the list directly
        if isinstance(data, dict) and 'data' in data:
            actual_data_points = data.get('data', [])
        elif isinstance(data, list):
            actual_data_points = data
        else:
            print("Unexpected data format from API.")
            return None
            
        print(f"Successfully fetched data. Number of points: {len(actual_data_points)}")
        return actual_data_points
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
    except ValueError as e:
        print(f"Error decoding JSON response: {e}")
        print(f"Response text: {response.text if 'response' in locals() else 'No response object'}")
    return None

def calculate_and_store_ois(raw_data):
    """
    Calculates a 1-month OIS rate proxy from Fed Funds futures data using daily compounding
    and stores it in DynamoDB. Processes the last 3 data points.
    """
    if not raw_data or len(raw_data) == 0:
        print("No raw data to process.")
        return

    points_to_process = raw_data[-3:]
    print(f"Processing the last {len(points_to_process)} data points for OIS calculation.")

    items_to_put = []
    for point in points_to_process:
        try:
            timestamp_ms = int(point[0])
            close_price = float(point[1]) # This is the futures price, e.g., 95.67

            # Step 1: Calculate the implied annual Fed Funds rate (decimal) from the futures price
            # R = (100 - P_future) / 100
            implied_annual_ff_rate_decimal = (100.0 - close_price) / 100.0

            # Step 2: Calculate the equivalent daily rate
            # r_daily = R_annual / 360 (assuming ACT/360)
            daily_rate_decimal = implied_annual_ff_rate_decimal / 360.0

            # Step 3: Compound daily for n days (e.g., 30 for 1-month) and annualize
            # OIS_rate = ((1 + r_daily)^n - 1) * (360 / n)
            n_days = 30.0 # Number of days in the OIS period (e.g., 1-month)
            
            if 1 + daily_rate_decimal <= 0: # Avoid math error with log or negative base for power
                print(f"Skipping point {point} due to invalid daily_rate_decimal: {daily_rate_decimal}")
                continue

            compounded_rate_decimal = ((1 + daily_rate_decimal)**n_days - 1) * (360.0 / n_days)

            # Step 4: Convert to percentage for storage/display
            ois_rate_to_store_percent = compounded_rate_decimal * 100.0

            # Prepare item for DynamoDB
            item = {
                'metricId': {'S': METRIC_ID},
                'timestamp': {'N': str(timestamp_ms)}, # Timestamp of the futures data point
                'value': {'N': f"{ois_rate_to_store_percent:.4f}"} # Store with 4 decimal places
            }
            items_to_put.append({'PutRequest': {'Item': item}})
            
            # Updated datetime conversion for future-proofing
            try:
                # For Python 3.11+ datetime.UTC is available
                dt_object = datetime.fromtimestamp(timestamp_ms / 1000.0, datetime.UTC)
            except AttributeError:
                # For older Python versions, use timezone.utc
                dt_object = datetime.fromtimestamp(timestamp_ms / 1000.0, timezone.utc)

            print(f"Prepared OIS data: Timestamp: {dt_object.strftime('%Y-%m-%d %H:%M:%S')} UTC, "
                  f"Futures Close: {close_price}, Implied FF Rate (ann.): {implied_annual_ff_rate_decimal*100:.4f}%, "
                  f"Calculated OIS (1M ann.): {ois_rate_to_store_percent:.4f}%")

        except (IndexError, ValueError, TypeError) as e:
            print(f"Error processing data point {point}: {e}")
            continue

    if items_to_put:
        try:
            for req in items_to_put: # Simpler PutItem calls for a few items
                dynamodb_client.put_item(
                    TableName=DYNAMODB_TABLE_NAME,
                    Item=req['PutRequest']['Item']
                )
            print(f"Successfully stored/updated {len(items_to_put)} OIS data points in DynamoDB.")
        except Exception as e:
            print(f"Error storing data in DynamoDB: {e}") # This is where you had 'Unable to locate credentials'
    else:
        print("No valid OIS data points were prepared for storage.")


if __name__ == "__main__":
    print("Starting OIS Fetcher Script...")
    # Ensure AWS credentials are configured if running locally (e.g., via `aws configure` or env vars)
    # For example, set environment variables:
    # os.environ['AWS_ACCESS_KEY_ID'] = "YOUR_KEY"
    # os.environ['AWS_SECRET_ACCESS_KEY'] = "YOUR_SECRET"
    # os.environ['AWS_DEFAULT_REGION'] = "eu-north-1"
    # (Better to configure them outside the script or use aws configure)

    raw_ff_futures_data = fetch_ois_data()
    if raw_ff_futures_data:
        calculate_and_store_ois(raw_ff_futures_data)
    print("OIS Fetcher Script finished.")
