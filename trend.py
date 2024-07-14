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



df=df_daily

import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt

# Filter
df_daily_trend = df

# Calculate indicators
adx = ta.adx(df_daily_trend['high'], df_daily_trend['low'], df_daily_trend['close'])
amat = ta.amat(df_daily_trend['close'])
aroon = ta.aroon(df_daily_trend['high'], df_daily_trend['low'])
chop = ta.chop(df_daily_trend['high'], df_daily_trend['low'], df_daily_trend['close'])
cksp = ta.cksp(df_daily_trend['high'], df_daily_trend['low'], df_daily_trend['close'])
decreasing = ta.decreasing(df_daily_trend['close'])
dpo = ta.dpo(df_daily_trend['close'], lookahead=False)
increasing = ta.increasing(df_daily_trend['close'])
qstick = ta.qstick(df_daily_trend['open'], df_daily_trend['close'])
tsignals = ta.tsignals(df_daily_trend['close'])
ttm_trend = ta.ttm_trend(df_daily_trend['close'], df_daily_trend['low'], df_daily_trend['high'])
vhf = ta.vhf(df_daily_trend['close'])
vortex = ta.vortex(df_daily_trend['high'], df_daily_trend['low'], df_daily_trend['close'])

# Function to add indicators to df_daily_trend with prefixes
def add_indicators_to_df(df, indicator, prefix):
    if isinstance(indicator, pd.DataFrame):
        for col in indicator.columns:
            df[f'{prefix}_{col}'] = indicator[col]
    else:  # Handle Series
        df[f'{prefix}'] = indicator

# Adding indicators to df_daily_trend with prefixes
add_indicators_to_df(df_daily_trend, adx, 'adx')
add_indicators_to_df(df_daily_trend, amat, 'amat')
add_indicators_to_df(df_daily_trend, aroon, 'aroon')
add_indicators_to_df(df_daily_trend, chop, 'chop')
add_indicators_to_df(df_daily_trend, cksp, 'cksp')
add_indicators_to_df(df_daily_trend, decreasing, 'decreasing')
add_indicators_to_df(df_daily_trend, dpo, 'dpo')
add_indicators_to_df(df_daily_trend, increasing, 'increasing')
add_indicators_to_df(df_daily_trend, qstick, 'qstick')
add_indicators_to_df(df_daily_trend, tsignals, 'tsignals')
add_indicators_to_df(df_daily_trend, ttm_trend, 'ttm_trend')
add_indicators_to_df(df_daily_trend, vhf, 'vhf')
add_indicators_to_df(df_daily_trend, vortex, 'vortex')

# Assuming df_daily_metrics is your DataFrame
df_daily_trend = df_daily_trend.sort_values(by='timestamp', ascending=False).head(1)
df_daily_trend

# Assuming df_daily_metrics is your DataFrame containing only the new row
# Example: df_daily_metrics = pd.DataFrame({...})

# Connect to Azure SQL Database
#connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=server;DATABASE=database;UID=your_username;PWD=password'
cnxn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
)

cursor = cnxn.cursor()

# Prepare the SQL INSERT statement dynamically
columns = [f'[{col}]' for col in df_daily_trend.columns.tolist()]  # Format column names
placeholders = ', '.join(['?'] * len(columns))  # Create placeholders for the values

insert_query = f"""
INSERT INTO df_daily_trend ({', '.join(columns)})
VALUES ({placeholders})
"""

# Get the values of the new row
new_row = [value.item() if isinstance(value, np.generic) else value for value in df_daily_trend.iloc[0].tolist()]

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


