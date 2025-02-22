# Nifty Stock Data Updater

This script fetches stock market data from Yahoo Finance for specified Nifty stock symbols and updates a PostgreSQL database with relevant trading metrics. The script continuously updates the database at 1-minute intervals.

## Features
- Connects to a PostgreSQL database.
- Creates necessary tables (`symbols`, `nifty_data`) if they do not exist.
- Fetches stock data from Yahoo Finance (`yfinance`).
- Computes:
  - Exponential Moving Average (EMA)
  - Volume Weighted Average Price (VWAP)
  - Average volume over 20 days
- Logs execution details to a file.

## Prerequisites
### 1. Install Dependencies
Ensure you have Python installed (3.7+ recommended) and install the required libraries:
```sh
pip install yfinance psycopg2 logging
```

### 2. PostgreSQL Database Setup
Ensure PostgreSQL is installed and running. Update the database connection details in `db_config`:
```python
# Database configuration
 db_config = {
    'host': 'your_host',
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'port': 'your_port'
}
```

## How to Use
### 1. Adding Symbols to Track
Insert symbols into the `symbols` table manually using SQL:
```sql
INSERT INTO symbols (symbol) VALUES ('RELIANCE');
INSERT INTO symbols (symbol) VALUES ('TCS');
```

### 2. Running the Script
Execute the script to start fetching and updating stock data:
```sh
python script_name.py
```

The script will:
1. Create tables if they do not exist.
2. Fetch stock symbols from the database.
3. Retrieve real-time stock data from Yahoo Finance.
4. Calculate additional metrics (EMA, VWAP, average volume).
5. Insert the data into the `nifty_data` table.
6. Repeat the process every minute.

## Logging
Logs are saved at:
```
C:\Apps\nifty_data_update.log
```
It records database connections, errors, and stock updates.

## Stopping the Script
Use `CTRL+C` to stop execution.

## Troubleshooting
- **Database connection error**: Ensure PostgreSQL is running and credentials are correct.
- **No symbols found**: Add symbols to the `symbols` table.
- **yfinance API failure**: Check for network issues or symbol correctness.

## License
This project is open-source and free to use.

