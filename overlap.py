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
#cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")

    
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

df_daily_overlap=df_daily



# Calculate various moving averages and indicators
df_daily_overlap['alma'] = ta.alma(df_daily_overlap['close'], length=9, offset=0.85)
df_daily_overlap['dema'] = ta.dema(df_daily_overlap['close'], length=20)
df_daily_overlap['ema'] = ta.ema(df_daily_overlap['close'], length=20)
df_daily_overlap['fwma'] = ta.fwma(df_daily_overlap['close'], length=20)
#df_daily_overlap['hilo'] = ta.hilo(df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'])
df_daily_overlap['hl2'] = ta.hl2(df_daily_overlap['high'], df_daily_overlap['low'])
df_daily_overlap['hlc3'] = ta.hlc3(df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'])
df_daily_overlap['hma'] = ta.hma(df_daily_overlap['close'], length=20)
df_daily_overlap['hwma'] = ta.hwma(df_daily_overlap['close'], length=20)
ichimoku_df = ta.ichimoku(df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'], lookahead=False)
#df_daily_overlap = pd.concat([df_daily_overlap, ichimoku_df], axis=1)  # Merging Ichimoku results
#df_daily_overlap['jma'] = ta.jma(df_daily_overlap['close'], length=20, phase=0)
df_daily_overlap['kama'] = ta.kama(df_daily_overlap['close'], length=20)
df_daily_overlap['linreg'] = ta.linreg(df_daily_overlap['close'], length=14)
#df_daily_overlap['mcgd'] = ta.mcgd(df_daily_overlap['close'], length=14)
df_daily_overlap['midpoint'] = ta.midpoint(df_daily_overlap['close'], length=14)
df_daily_overlap['midprice'] = ta.midprice(df_daily_overlap['high'], df_daily_overlap['low'])
df_daily_overlap['ohlc4'] = ta.ohlc4(df_daily_overlap['open'], df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'])
df_daily_overlap['pwma'] = ta.pwma(df_daily_overlap['close'], length=20)
df_daily_overlap['rma'] = ta.rma(df_daily_overlap['close'], length=14)
df_daily_overlap['sinwma'] = ta.sinwma(df_daily_overlap['close'], length=20)
df_daily_overlap['sma'] = ta.sma(df_daily_overlap['close'], length=20)
df_daily_overlap['ssf'] = ta.ssf(df_daily_overlap['close'], length=14)
#df_daily_overlap['superoverlap'] = ta.superoverlap(df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'], length=7, multiplier=3)
df_daily_overlap['swma'] = ta.swma(df_daily_overlap['close'], length=20)
df_daily_overlap['t3'] = ta.t3(df_daily_overlap['close'], length=14, vfactor=0)
df_daily_overlap['tema'] = ta.tema(df_daily_overlap['close'], length=20)
df_daily_overlap['trima'] = ta.trima(df_daily_overlap['close'], length=14)
df_daily_overlap['vidya'] = ta.vidya(df_daily_overlap['close'], length=20)
#df_daily_overlap['vwap'] = ta.vwap(df_daily_overlap['close'], df_daily_overlap['volume'])  # Ensure 'volume' is available
df_daily_overlap['vwma'] = ta.vwma(df_daily_overlap['close'], df_daily_overlap['volume'], length=20)
df_daily_overlap['wcp'] = ta.wcp(df_daily_overlap['open'], df_daily_overlap['high'], df_daily_overlap['low'], df_daily_overlap['close'])
df_daily_overlap['wma'] = ta.wma(df_daily_overlap['close'], length=20)
df_daily_overlap['zlma'] = ta.zlma(df_daily_overlap['close'], length=20)

# Now df_daily_overlap contains all the calculated indicators


# Assuming df_daily_metrics is your DataFrame
df_daily_overlap = df_daily_overlap.sort_values(by='timestamp', ascending=False).head(1)

# Assuming df_daily_metrics is your DataFrame containing only the new row
# Example: df_daily_metrics = pd.DataFrame({...})

# Connect to Azure SQL Database
#connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=database;UID=your_username;PWD=password'
cnxn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

cursor = cnxn.cursor()

# Prepare the SQL INSERT statement dynamically
columns = [f'[{col}]' for col in df_daily_overlap.columns.tolist()]  # Format column names
placeholders = ', '.join(['?'] * len(columns))  # Create placeholders for the values

insert_query = f"""
INSERT INTO df_daily_overlap ({', '.join(columns)})
VALUES ({placeholders})
"""

# Get the values of the new row
new_row = df_daily_overlap.iloc[0].tolist()  # Convert the first row to a list

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
