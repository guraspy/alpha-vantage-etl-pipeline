"""
ETL pipeline for daily stock data (AAPL, GOOG, MSFT) using Alpha Vantage API.
Steps:
1. Extract - Fetch daily stock data and save raw JSON.
2. Transform - Clean data with pandas.
3. Load - Insert into SQLite database.
"""

import os
import requests
import json
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from pydantic import BaseModel, Field, ValidationError

# CONFIGURATION
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
SYMBOLS = ['AAPL', 'GOOG', 'MSFT']
BASE_URL = 'https://www.alphavantage.co/query'
DB_NAME = 'stock_data.db'
RAW_DATA_PATH = Path('raw_data')

# create raw_data directory if it doesn't exist
RAW_DATA_PATH.mkdir(exist_ok=True)

# DATA VALIDATION WITH PYDANTIC 
class StockDataPoint(BaseModel):
    """
    Pydantic model to validate one day's stock data.
    """
    open: float = Field(..., alias='1. open')
    high: float = Field(..., alias='2. high')
    low: float = Field(..., alias='3. low')
    close: float = Field(..., alias='4. close')
    volume: int = Field(..., alias='5. volume')

class TimeSeriesData(BaseModel):
    """
    Pydantic model to validate the 'TIME_SERIES_DAILY' structure.
    """
    time_series: Dict[str, StockDataPoint] = Field(..., alias='Time Series (Daily)')

# Extract

def fetch_stock_data(symbol: str) -> dict:
    """
    Fetches daily stock data for a given symbol and save the raw JSON.
    """
    print(f"Fetching data for {symbol}")
    # check if we have fetched data before to decide output size
    # 'compact' returns only the latest 100 data points
    # 'full' returns the full-length time series of 20+ years of historical data.
    output_size = 'compact' if list(RAW_DATA_PATH.glob(f'{symbol}_*.json')) else 'full'
    
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': API_KEY,
        'outputsize': output_size
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # raise an exception for bad status codes (4xx or 5xx)
        data = response.json()

        # checks api rate limit 
        if "Information" in data:
            raise RuntimeError(f"API rate limit reached: {data['Information']}")

        # validate the response structure and data types using Pydantic
        TimeSeriesData.model_validate(data)

        # save the raw data
        today = datetime.now().strftime('%Y-%m-%d')
        file_path = RAW_DATA_PATH / f'{symbol}_{today}.json'
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Successfully fetched and saved raw data to {file_path}")
        return file_path
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
    except ValidationError as e:
        print(f"Data validation error for {symbol}: {e}")
    except KeyError:
        print(f"Error: Unexpected JSON structure from API for {symbol}. Response: {data}")
    return None

# Transform

def transform_data(file_path: Path, symbol: str) -> pd.DataFrame:
    """
    Convert JSON data into a clean pandas DataFrame.
    """
    print(f"Transforming data for {symbol} from {file_path}")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')
        
        # rename columns
        df.rename(columns={
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }, inplace=True)
        
        # convert columns to appropriate numeric types
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col])
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').astype('Int64')
        
        # reset index to make 'date' a column
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'date'}, inplace=True)
        df['date'] = pd.to_datetime(df['date']).dt.date
        
        # add symbol, extraction timestamp and daily change percentage columns
        df['symbol'] = symbol
        df['extraction_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['daily_change_percentage'] = ((df['close'] - df['open']) / df['open']) * 100
        
        print(f"Transformation for {symbol} successful. Shape: {df.shape}")
        return df

    except (FileNotFoundError, KeyError, TypeError) as e:
        print(f"Error transforming data for {symbol}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


# Load

def setup_database():
    """
    Creates the SQLite database and table if they don't exist.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_daily_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL,
            daily_change_percentage REAL,
            extraction_timestamp TIMESTAMP NOT NULL,
            UNIQUE(symbol, date)
        )
    ''')
    conn.commit()
    conn.close()
    print("Database setup complete. Table 'stock_daily_data' is ready.\n")

def load_data_to_db(df: pd.DataFrame):
    """
    Load transformed DataFrame into the SQLite.
    The UNIQUE constraint on (symbol, date) skipping duplicates.
    """
    if df.empty:
        print("No data to load.")
        return
        
    print(f"Loading data for symbols {df['symbol'].unique()} into {DB_NAME}")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # prepare data for insertion
    records_to_insert = [tuple(row) for row in df[[
        'symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
        'daily_change_percentage', 'extraction_timestamp'
    ]].itertuples(index=False)]
    
    # use INSERT OR IGNORE to skip duplicates based on the UNIQUE constraint
    insert_query = '''
        INSERT OR IGNORE INTO stock_daily_data (
            symbol, date, open_price, high_price, low_price, close_price, volume,
            daily_change_percentage, extraction_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    
    cursor.executemany(insert_query, records_to_insert)
    conn.commit()
    
    print(f"Successfully loaded {cursor.rowcount} new records into the database.\n")
    conn.close()

# MAIN

def run_etl_pipeline():
    """
    Main function to run the ETL pipeline for all symbols.
    """
    print("\nStarting ETL pipeline...\n")
    setup_database()
    
    for symbol in SYMBOLS:
        raw_file_path = fetch_stock_data(symbol)
        if raw_file_path:
            transformed_df = transform_data(raw_file_path, symbol)
            load_data_to_db(transformed_df)
    
    print("ETL pipeline finished successfully.")


if __name__ == "__main__":
    if not API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set.")
    run_etl_pipeline()