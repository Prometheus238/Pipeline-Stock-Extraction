from datetime import datetime 
import json
import yfinance as yf
import psycopg2 # type: ignore

def get_stock_data(symbols, start_date, end_date):
    all_data = {}
    try:
        for symbol in symbols:
            # Fetch data from Yahoo Finance API for each symbol
            data = yf.download(symbol, start=start_date, end=end_date)
            
            # Convert data to JSON format
            json_data = data.to_json(orient="index")
            
            # Add the data to the dictionary with symbol as key
            all_data[symbol] = json_data
        
        return all_data
    except Exception as e:
        print("Error fetching data:", e)
        return None
    
def insert_raw_data(cursor, stock_data):
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS raw_stock_data (symbol VARCHAR(10), raw JSONB)")

        for symbol, data in stock_data.items():
            cursor.execute("INSERT INTO raw_stock_data (symbol, raw) VALUES (%s, %s)", (symbol, json.dumps(data)))
        
        print("Raw stock data inserted successfully.")

    except Exception as e:
        print("Error inserting raw data to PostgreSQL:", e)


def insert_parsed_json(cursor, stock_data):
    try:
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol VARCHAR(10),
                date DATE,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT
            )
        """)

        # Rename columns to lowercase to match table columns (optional but safer)
        stock_data.rename(columns=str.lower, inplace=True)

        # Insert data row by row
        for index, row in stock_data.iterrows():
            cursor.execute("""
                INSERT INTO stock_data (symbol, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol,
                index.date(),         # date from the index
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                int(row['volume'])    # ensure volume is integer
            ))

        print("Parsed stock data inserted successfully.")

    except Exception as e:
        print("Error inserting data to PostgreSQL:", e)





if __name__ == "__main__":
    # Specify the list of stock symbols, start date, and end date
    stock_symbols = ["AAPL", "MSFT", "Meta", "NVDA", "GOOG", "AMZN", "INTC", "CRM", "TSM", "AMD"]
    start_date = "2025-01-01"
    end_date = "2025-07-31"
    
    # Call the function to fetch data
    stock_data = get_stock_data(stock_symbols, start_date, end_date)
    
    conn = psycopg2.connect(database="stock"
                            ,user="postgres"
                            ,password="Reed2308*#"
                            ,host="10.0.0.43"
                            ,port="5432")

    cursor = conn.cursor()
    
    if stock_data:
        print("Stock data fetched successfully:")
        insert_raw_data(cursor, stock_data)
        insert_parsed_json(cursor, stock_data)
        conn.commit()
        conn.close()
    else:
        print("Failed to fetch stock data.")

    


