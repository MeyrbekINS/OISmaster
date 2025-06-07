# OIS_Fetcher.py - Adapted for Yahoo Finance using yfinance

import yfinance as yf # Import yfinance
import boto3
import os
import time
from datetime import datetime, timezone, date, timedelta
# import requests # No longer needed directly if yfinance handles requests

# Configuration
# --- IMPORTANT: Verify this is the correct Yahoo Finance ticker ---
FED_FUNDS_FUTURES_TICKER = os.environ.get('FED_FUNDS_FUTURES_TICKER', 'ZQ=F')

DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'OISRATES')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')

# Metric IDs
METRIC_ID_OIS = os.environ.get('METRIC_ID_OIS', 'CALCULATED_OIS_1M_RATE')
METRIC_ID_IMPLIED_FF = os.environ.get('METRIC_ID_IMPLIED_FF', 'IMPLIED_FF_RATE')

dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)

def fetch_fed_funds_futures_data():
    """
    Fetches historical Fed Funds futures data from Yahoo Finance using yfinance.
    We need the last few days of data.
    """
    print(f"Fetching Fed Funds futures data for ticker: {FED_FUNDS_FUTURES_TICKER} from Yahoo Finance")
    try:
        ticker = yf.Ticker(FED_FUNDS_FUTURES_TICKER)
        
        # Fetch historical data. yfinance returns a pandas DataFrame.
        # Let's try to get data for the last ~10 days to ensure we have the last 3 trading days.
        # Note: For futures, 'period' might be tricky. Using start/end is more reliable.
        end_date = date.today()
        start_date = end_date - timedelta(days=10) # Get a small window
        
        # interval="1d" for daily data
        hist_df = ticker.history(start=start_date.strftime('%Y-%m-%d'), 
                                 end=end_date.strftime('%Y-%m-%d'), 
                                 interval="1d")

        if hist_df.empty:
            print(f"No historical data found for {FED_FUNDS_FUTURES_TICKER} for the given period.")
            return None

        print(f"Successfully fetched {len(hist_df)} data points from Yahoo Finance.")
        # We need the 'Close' price and the 'Date' (timestamp)
        # The DataFrame index is usually the Datetime object for the date.
        # Example relevant columns: hist_df.index (for date), hist_df['Close']
        return hist_df

    except Exception as e:
        # yfinance can raise various exceptions, including for invalid tickers or network issues
        print(f"Error fetching data from Yahoo Finance for {FED_FUNDS_FUTURES_TICKER}: {e}")
    return None

def calculate_and_store_rates(historical_data_df):
    """
    Calculates 1-month OIS rate proxy and Implied FF Rate from Yahoo Finance data (pandas DataFrame)
    and stores them in DynamoDB. Processes the last 3 available data points from the DataFrame.
    """
    if historical_data_df is None or historical_data_df.empty:
        print("No historical data DataFrame to process.")
        return

    # Get the last 3 available data points
    # The DataFrame is usually sorted chronologically by yfinance
    points_to_process_df = historical_data_df.tail(60)
    
    if points_to_process_df.empty:
        print("Not enough data points after tail(3) to process.")
        return

    print(f"Processing the last {len(points_to_process_df)} data points for OIS and Implied FF Rate calculation.")

    items_to_put = []
    for index_date, row in points_to_process_df.iterrows():
        try:
            # The index of the DataFrame row (index_date) is a pandas Timestamp object (datetime-like)
            # Convert pandas Timestamp to UTC milliseconds epoch
            # Ensure it's treated as UTC if it's naive, or convert to UTC
            if index_date.tzinfo is None:
                dt_object_utc = index_date.tz_localize('UTC') # Assume date is UTC if naive, or adjust based on Yahoo's source timezone
            else:
                dt_object_utc = index_date.tz_convert('UTC')
            
            timestamp_ms = int(dt_object_utc.timestamp() * 1000)
            
            close_price = float(row['Close']) # Get the 'Close' price from the row

            # --- Calculations remain the same ---
            implied_annual_ff_rate_decimal = (100.0 - close_price) / 100.0
            implied_ff_rate_to_store_percent = implied_annual_ff_rate_decimal * 100.0

            item_ff = {
                'metricId': {'S': METRIC_ID_IMPLIED_FF},
                'timestamp': {'N': str(timestamp_ms)},
                'value': {'N': f"{implied_ff_rate_to_store_percent:.4f}"}
            }
            items_to_put.append({'PutRequest': {'Item': item_ff}})

            daily_rate_decimal = implied_annual_ff_rate_decimal / 360.0
            n_days = 30.0
            ois_rate_to_store_percent = None
            if 1 + daily_rate_decimal > 0:
                compounded_rate_decimal = ((1 + daily_rate_decimal)**n_days - 1) * (360.0 / n_days)
                ois_rate_to_store_percent = compounded_rate_decimal * 100.0
                
                item_ois = {
                    'metricId': {'S': METRIC_ID_OIS},
                    'timestamp': {'N': str(timestamp_ms)},
                    'value': {'N': f"{ois_rate_to_store_percent:.4f}"}
                }
                items_to_put.append({'PutRequest': {'Item': item_ois}})
            else:
                print(f"Skipping OIS calculation for date {index_date.strftime('%Y-%m-%d')} due to invalid daily_rate_decimal.")

            print(f"Data for Timestamp: {dt_object_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC, Futures Close: {close_price}")
            print(f"  Implied FF Rate (ann.): {implied_ff_rate_to_store_percent:.4f}%")
            if ois_rate_to_store_percent is not None:
                print(f"  Calculated OIS (1M ann.): {ois_rate_to_store_percent:.4f}%")

        except (KeyError, ValueError, TypeError) as e:
            print(f"Error processing data row for date {index_date.strftime('%Y-%m-%d') if 'index_date' in locals() else 'unknown'}: {e}")
            continue
    # ... (DynamoDB storage part remains the same) ...
    if items_to_put:
        try:
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
    print("Starting OIS & Implied FF Rate Fetcher Script (Yahoo Finance)...")
    historical_data = fetch_fed_funds_futures_data()
    if historical_data is not None:
        calculate_and_store_rates(historical_data)
    print("OIS & Implied FF Rate Fetcher Script (Yahoo Finance) finished.")
