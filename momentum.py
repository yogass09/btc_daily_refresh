import pandas_ta as ta

import time
# Start time
start_time = time.time()
import pandas as pd
import pandas_ta as ta
import pyodbc
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Define connection parameters
server = 'cp-io-sql.database.windows.net'
database = 'sql_db_ohlcv'
username = 'yogass09'
password = 'Qwerty@312'
driver = 'ODBC Driver 17 for SQL Server'
port = 1433

# Establish the connection
conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

# Create a cursor object
cursor = conn.cursor()

# Execute a query to fetch the list of tables
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")

# Fetch all results
tables = cursor.fetchall()

# Process the results to create a list of table names
table_list = [table[0] for table in tables]

# Print the list of tables
print("Available tables:")
for table in table_list:
    print(table)
    
    
    # Load the btc_top_charts table into a DataFrame
query = 'SELECT * FROM [dbo].[btc_daily]'
df = pd.read_sql(query, conn)


# Columns to keep
columns_to_keep = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'market_cap']

# Filter the DataFrame to keep only the desired columns
df_daily = df[columns_to_keep]

# Convert the 'timestamp' column to datetime if it is not already
df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])

# Get the date 365 days ago from today
date_threshold = datetime.now() - timedelta(days=720)

# Filter the DataFrame to include only the last 365 days
df_daily = df_daily[df_daily['timestamp'] >= date_threshold]

print("Filtered DataFrame:")
print(df_daily)


df=df_daily

df_daily_momentum=df_daily

import pandas_ta as ta

# OHLC4
df_daily_momentum['ohlc4'] = ta.ohlc4(df_daily_momentum['open'], df_daily_momentum['high'], df_daily_momentum['low'], df_daily_momentum['close'])

# HL2
df_daily_momentum['hl2'] = ta.hl2(df_daily_momentum['high'], df_daily_momentum['low'])

# Linear Regression
df_daily_momentum['linreg'] = ta.linreg(df_daily_momentum['close'])

# Parabolic SAR
#df_daily_momentum['psar'] = ta.psar(df_daily_momentum['high'], df_daily_momentum['low'])

# Weighted Close Price
df_daily_momentum['wcp'] = ta.wcp(df_daily_momentum['high'], df_daily_momentum['low'], df_daily_momentum['close'])

# Forward Weighted Moving Average
df_daily_momentum['fwma'] = ta.fwma(df_daily_momentum['close'])

# Zero Lag Moving Average
df_daily_momentum['zlma'] = ta.zlma(df_daily_momentum['close'])

# Super Smooth Filter
df_daily_momentum['ssf'] = ta.ssf(df_daily_momentum['close'])

# Double Exponential Moving Average
df_daily_momentum['dema'] = ta.dema(df_daily_momentum['close'])

# Hull Moving Average
df_daily_momentum['hma'] = ta.hma(df_daily_momentum['close'])

# Midpoint
df_daily_momentum['midpoint'] = ta.midpoint(df_daily_momentum['close'])

# Triple Exponential Moving Average
df_daily_momentum['tema'] = ta.tema(df_daily_momentum['close'])

# Half-Weighted Moving Average
df_daily_momentum['hwma'] = ta.hwma(df_daily_momentum['close'])

# Jurik Moving Average
#df_daily_momentum['jma'] = ta.jma(df_daily_momentum['close'],length = 108)

# Qstick
df_daily_momentum['qstick'] = ta.qstick(df_daily_momentum['close'], df_daily_momentum['open'])


# Assuming df_daily_metrics is your DataFrame
df_daily_momentum = df_daily_momentum.sort_values(by='timestamp', ascending=False).head(1)
df_daily_momentum

# Assuming df_daily_metrics is your DataFrame containing only the new row
# Example: df_daily_metrics = pd.DataFrame({...})

# Connect to Azure SQL Database
#connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=database;UID=your_username;PWD=password'
cnxn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

cursor = cnxn.cursor()

# Prepare the SQL INSERT statement dynamically
columns = [f'[{col}]' for col in df_daily_momentum.columns.tolist()]  # Format column names
placeholders = ', '.join(['?'] * len(columns))  # Create placeholders for the values

insert_query = f"""
INSERT INTO df_daily_momentum ({', '.join(columns)})
VALUES ({placeholders})
"""

# Get the values of the new row
new_row = df_daily_momentum.iloc[0].tolist()  # Convert the first row to a list

# Execute the insert query with the new row's values
cursor.execute(insert_query, *new_row)

# Commit the transaction
cnxn.commit()

# Close the cursor and connection
cursor.close()
cnxn.close()

# End time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time
print(f"Elapsed time: {elapsed_time:.2f} seconds")


