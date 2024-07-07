# Record the start time
start_time <- Sys.time()

# Libraries
library(DBI)
library(odbc)
library(dplyr)
library(crypto2)

# Daily Refresh for BTC
coin_list_all <- crypto_list(only_active = TRUE)

btc_daily<-crypto_history(coin_list = coin_list_all,convert = "USD",limit = 1,
                          sleep = 0, start_date = Sys.Date()-5, end_date = Sys.Date())

# Set up your Azure SQL Database connection
con <- dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server",
                 Server = "cp-io-sql.database.windows.net", Database = "sql_db_ohlcv",
                 UID = "yogass09", PWD = "Qwerty@312",Port = 1433)


# Write data to the SQL database
dbWriteTable(con, "btc_daily", btc_daily, append = TRUE)

# Disconnect from the database
dbDisconnect(con)
