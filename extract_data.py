from datetime import datetime 
import json
import yfinance as yf
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_stock_data(symbols, start_date, end_date):
    all_data = {}
    try:
        for symbol in symbols:
            data = yf.download(symbol, start=start_date, end=end_date)
            all_data[symbol] = data  # Store as DataFrame, not JSON
        return all_data
    except Exception as e:
        print("Error fetching data:", e)
        return None

def insert_raw_data(cursor, stock_data):
    try:
        cursor.execute("DROP TABLE IF EXISTS raw_stock_data")
        cursor.execute("CREATE TABLE raw_stock_data (symbol VARCHAR(10), raw JSONB)")

        for symbol, df in stock_data.items():
            json_data = df.to_json(orient="index")
            cursor.execute(
                "INSERT INTO raw_stock_data (symbol, raw) VALUES (%s, %s)",
                (symbol, json.dumps(json_data))
            )
        
        print("Raw stock data inserted successfully.")

    except Exception as e:
        print("Error inserting raw data to PostgreSQL:", e)

def insert_parsed_json(cursor, stock_data):
    try:
        cursor.execute("DROP TABLE IF EXISTS stock_data")
        cursor.execute("""
            CREATE TABLE stock_data (
                symbol VARCHAR(10),
                date DATE,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            )
        """)

        for symbol, df in stock_data.items():
            # Flatten MultiIndex if needed
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            df.rename(columns=str.lower, inplace=True)

            for index, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    symbol,
                    index.date(),
                    float(row['open']) if pd.notna(row['open']) else None,
                    float(row['high']) if pd.notna(row['high']) else None,
                    float(row['low']) if pd.notna(row['low']) else None,
                    float(row['close']) if pd.notna(row['close']) else None,
                    int(row['volume']) if pd.notna(row['volume']) else None
                ))

        print("Parsed stock data inserted successfully.")

    except Exception as e:
        print("Error inserting data to PostgreSQL:", e)


if __name__ == "__main__":

    # Calculate date range
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

    print(f"Fetching stock data from {start_date} to {end_date}...")
    stock_symbols = ["AAPL", "MSFT", "META", "NVDA", "GOOG", "AMZN", "INTC", "CRM", "TSM", "AMD"]

    stock_data = get_stock_data(stock_symbols, start_date, end_date)

    conn = psycopg2.connect(
        database="stock",
        user="postgres",
        password="Reed2308*#",
        host="10.0.0.43",
        port="5432"
    )

    cursor = conn.cursor()

    if stock_data:
        print("Stock data fetched successfully:")
        insert_raw_data(cursor, stock_data)
        insert_parsed_json(cursor, stock_data)
        conn.commit()
        conn.close()
    else:
        print("Failed to fetch stock data.")
