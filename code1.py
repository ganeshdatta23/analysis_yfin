import yfinance as yf
import logging
import os
import psycopg2
from psycopg2 import sql
from time import sleep
from datetime import datetime

# Database configuration
db_config = {
    'host': 'your_host',
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'port': 'your_port'
}

# Configure logging
log_file_path = os.path.join("C:", os.sep, "Apps", "nifty_data_update.log")
os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
logging.basicConfig(filename=log_file_path, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db():
    """Establishes a connection to the database."""
    try:
        connection = psycopg2.connect(**db_config)
        return connection
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def create_table():
    """Creates the tables if they do not exist."""
    connection = connect_db()
    if not connection:
        return
    try:
        cursor = connection.cursor()
        # Create symbols table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) UNIQUE NOT NULL
        );
        """)
        # Create nifty_data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS nifty_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20),
            timestamp TIMESTAMP,
            price NUMERIC(10, 2),
            open NUMERIC(10, 2),
            high NUMERIC(10, 2),
            low NUMERIC(10, 2),
            close NUMERIC(10, 2),
            volume BIGINT,
            avg_volume NUMERIC(10, 2),
            ema NUMERIC(10, 2),
            vwap NUMERIC(10, 2)
        );
        """)
        connection.commit()
        logging.info("Tables created successfully.")
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error creating tables: {e}")

def get_symbols():
    """Fetches all symbols from the database."""
    connection = connect_db()
    if not connection:
        return []
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT symbol FROM symbols;")
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()
        return symbols
    except Exception as e:
        logging.error(f"Error fetching symbols: {e}")
        return []

def get_stock_data(symbol, period):
    """Fetches stock data from yfinance."""
    try:
        data = yf.download(symbol + ".NS", period=period)
        if data.empty:
            logging.warning(f"No data found for {symbol} for period {period}")
            return None
        return data
    except Exception as e:
        logging.error(f"Error getting data for {symbol}: {e}")
        return None

def calculate_average_volume(data):
    """Calculates the average volume."""
    return data['Volume'].mean() if data is not None else None

def calculate_ema(data, period=20):
    """Calculates the Exponential Moving Average (EMA)."""
    return data['Close'].ewm(span=period, adjust=False).mean() if data is not None else None

def calculate_vwap(data):
    """Calculates the Volume Weighted Average Price (VWAP)."""
    try:
        vwap = (data['Close'] * data['Volume']).cumsum() / data['Volume'].cumsum()
        return vwap.iloc[-1]
    except KeyError:
        return None

def update_nifty_data(symbol):
    """Updates Nifty data in the database."""
    connection = connect_db()
    if not connection:
        return
    
    cursor = connection.cursor()
    try:
        data_1d = get_stock_data(symbol, "1d")
        data_20d = get_stock_data(symbol, "20d")
        
        if data_1d is None or data_20d is None:
            return
        
        price = data_1d['Close'].iloc[-1].round(2)
        open_price = data_1d['Open'].iloc[-1].round(2)
        high = data_1d['High'].iloc[-1].round(2)
        low = data_1d['Low'].iloc[-1].round(2)
        close = data_1d['Close'].iloc[-1].round(2)
        volume = data_1d['Volume'].iloc[-1]
        avg_volume = calculate_average_volume(data_20d)
        ema_20 = calculate_ema(data_20d)
        ema_value = ema_20.iloc[-1] if ema_20 is not None else None
        vwap = calculate_vwap(data_1d)
        timestamp = datetime.now()
        
        insert_query = sql.SQL("""
        INSERT INTO nifty_data (symbol, timestamp, price, open, high, low, close, volume, avg_volume, ema, vwap)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)
        cursor.execute(insert_query, (symbol, timestamp, price, open_price, high, low, close, volume, avg_volume, ema_value, vwap))
        connection.commit()
        logging.info(f"Data for {symbol} updated successfully at {timestamp}.")
    except Exception as e:
        logging.error(f"Error updating data for {symbol}: {e}")
    finally:
        cursor.close()
        connection.close()

def main():
    """Fetch and update data for all symbols every 1 minute."""
    create_table()
    while True:
        symbols = get_symbols()
        if not symbols:
            logging.warning("No symbols found. Add symbols to the database.")
            sleep(60)
            continue
        
        for symbol in symbols:
            update_nifty_data(symbol)
        sleep(60)

if __name__ == "__main__":
    main()