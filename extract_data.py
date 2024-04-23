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
        cursor.execute("CREATE TABLE raw_stock_data (symbol VARCHAR(10), raw JSONB)")

        for symbol, data in stock_data.items():
            cursor.execute("INSERT INTO raw_stock_data (symbol, raw) VALUES (%s, %s)", (symbol, json.dumps(data)))
        
        print("Raw stock data inserted successfully.")

    except Exception as e:
        print("Error inserting raw data to PostgreSQL:", e)


def insert_parsed_json(cursor, stock_data):
    try:
        # Create table if not exists
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
        
        # Insert data into the table
        for symbol, data in stock_data.items():
            parsed_data = json.loads(data)  # Convert JSON string back to dictionary
            for index, row in parsed_data.items():
                # Convert date from milliseconds since epoch to PostgreSQL-compatible format
                date = datetime.fromtimestamp(int(index) / 1000).strftime('%Y-%m-%d')
                cursor.execute("INSERT INTO stock_data (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)", (symbol, date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
        
        print("Parsed stock data inserted successfully.")

    except Exception as e:
        print("Error inserting data to PostgreSQL:", e)





if __name__ == "__main__":
    # Specify the list of stock symbols, start date, and end date
    stock_symbols = ["AAPL", "MSFT", "Meta", "NVDA", "GOOG", "AMZN", "INTC", "CRM", "TSM", "AMD"]
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    
    # Call the function to fetch data
    stock_data = get_stock_data(stock_symbols, start_date, end_date)
    
    conn = psycopg2.connect(database="stock"
                            ,user="postgres"
                            ,password="Reed2308*#"
                            ,host="localhost"
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

    


