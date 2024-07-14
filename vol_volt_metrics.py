# Start time
import time
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
date_threshold = datetime.now() - timedelta(days=365)

# Filter the DataFrame to include only the last 365 days
df_daily = df_daily[df_daily['timestamp'] >= date_threshold]

print("Filtered DataFrame:")
print(df_daily)

df=df_daily



df_daily = pd.DataFrame(df_daily)

import pandas as pd


# Ensure 'timestamp' is a datetime
df_daily['timestamp'] = pd.to_datetime(df_daily['timestamp'])

# Duplicate the 'timestamp' column
df_daily['timestamp_duplicate'] = df_daily['timestamp']

# Set 'timestamp' as the index
df_daily.set_index('timestamp', inplace=True)



# Calculate  metrics
df_daily['log_return'] = ta.log_return(df_daily['close'], cumulative=False)
df_daily['percent_return'] = ta.percent_return(df_daily['close'], cumulative=False)
# Calculate cumulative metrics
results = ta.drawdown(df_daily['close'], cumulative=True)
df_daily['dd'] = results['DD_PCT']
df_daily['log_return_cum'] = ta.log_return(df_daily['close'], cumulative=True)
df_daily['percent_return_cum'] = ta.percent_return(df_daily['close'], cumulative=True)

# Calculate indicators and add volume prefix
df_daily['vol_cmf'] = ta.cmf(df_daily['high'], df_daily['low'], df_daily['close'], df_daily['volume'], length=20).round(2)  # Adjust length as needed
df_daily['vol_efi'] = ta.efi(df_daily['close'], df_daily['volume']).round(2)
df_daily['vol_eom'] = ta.eom(df_daily['high'], df_daily['low'], df_daily['volume'], df_daily['close']).round(2)
#df_daily['vol_mfi'] = ta.mfi(df_daily['high'], df_daily['low'], df_daily['close'], df_daily['volume'], length=14).round(2)  # Adjust length as needed
df_daily['vol_nvi'] = ta.nvi(df_daily['close'], df_daily['volume']).round(2)

# Calculate KVO
g = ta.kvo(df_daily['high'], df_daily['low'], df_daily['close'], df_daily['volume'], mamode="rsi").round(2)
df_daily['vol_KVOs'] = g['KVOs_34_55_13']

# Adding volume metrics with prefix
df_daily['vol_obv'] = ta.obv(df_daily['close'], df_daily['volume']).round(2)
df_daily['vol_pvi'] = ta.pvi(df_daily['close'], df_daily['volume']).round(2)
df_daily['vol_pvol'] = ta.pvol(df_daily['close'], df_daily['volume']).round(2)
df_daily['vol_pvr'] = ta.pvr(df_daily['close'], df_daily['volume']).round(2)
df_daily['vol_pvt'] = ta.pvt(df_daily['close'], df_daily['volume']).round(2)


#df_daily['aberration'] = ta.aberration(df_daily['high'], df_daily['low'], df_daily['close'])

# Acceleration Bands (ACCBANDS)
#df_daily['accbands'] = ta.accbands(df_daily['high'], df_daily['low'], df_daily['close'])

# Average True Range (ATR)
df_daily['volt_atr'] = ta.atr(df_daily['high'], df_daily['low'], df_daily['close'])

# Bollinger Bands (BBANDS)
#df_daily['bbands'] = ta.bbands(df_daily['close'])

# Donchian Channel
#df_daily['donchian'] = ta.donchian(df_daily['high'], df_daily['low'], df_daily['close'])

# Holt-Winter Channel (HWC)
#df_daily['hwc'] = ta.hwc(df_daily['close'])

# Keltner Channel (KC)
#df_daily['kc'] = ta.kc(df_daily['high'], df_daily['low'], df_daily['close'])

# Mass Index (MASSI)
df_daily['volt_massi'] = ta.massi(df_daily['high'], df_daily['low'])

# Normalized Average True Range (NATR)
df_daily['volt_natr'] = ta.natr(df_daily['high'], df_daily['low'], df_daily['close'])

# Price Distance (PDIST)
#df_daily['pdist'] = ta.pdist(df_daily['high'], df_daily['low'], df_daily['close'])

# Relative Volatility Index (RVI)
#df_daily['rvi'] = ta.rvi(df_daily['close'], df_daily['low'], df_daily['high'])

# Elder's Thermometer (THERMO)
#df_daily['thermo'] = ta.thermo(df_daily['close'])

# True Range (TRUE_RANGE)
df_daily['volt_true_range'] = ta.true_range(df_daily['high'], df_daily['low'], df_daily['close'])

# Ulcer Index (UI)
df_daily['colt_ui'] = ta.ui(df_daily['close'])



df_daily_metrics=df_daily

# Assuming df_daily_metrics is your DataFrame
df_daily_metrics = df_daily_metrics.sort_values(by='timestamp_duplicate', ascending=False).head(1)

# Assuming df_daily_metrics is your DataFrame containing only the new row
# Example: df_daily_metrics = pd.DataFrame({...})

# Connect to Azure SQL Database
#connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=database;UID=your_username;PWD=password'
cnxn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

cursor = cnxn.cursor()

# Prepare the SQL INSERT statement dynamically
columns = [f'[{col}]' for col in df_daily_metrics.columns.tolist()]  # Format column names
placeholders = ', '.join(['?'] * len(columns))  # Create placeholders for the values

insert_query = f"""
INSERT INTO df_daily_metrics ({', '.join(columns)})
VALUES ({placeholders})
"""

# Get the values of the new row
new_row = df_daily_metrics.iloc[0].tolist()  # Convert the first row to a list

# Execute the insert query with the new row's values
cursor.execute(insert_query, *new_row)

# Commit the transaction
cnxn.commit()

# Close the cursor and connection
cursor.close()
cnxn.close()

end_time = time.time()
time= end_time-start_time
time

import sys
print(sys.version)

