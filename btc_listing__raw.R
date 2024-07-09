library(DBI)
library(odbc)
library(dplyr)
library(crypto2)

##  Activate Later as script 
btc_listing_raw <-  crypto_listings(limit=1,which="historical", quote=TRUE,
                                       start_date = =Sys.Date()-1)


btc_listing_raw <- btc_listing_raw%>%
  select(
    last_updated,
    circulating_supply,
    total_supply,
    max_supply,
    volume24h,
    slug,
    percent_change24h,
    percent_change7d,
    price
  )



# Set up your Azure SQL Database connection
con <- dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server",
                 Server = "cp-io-sql.database.windows.net", Database = "sql_db_ohlcv",
                 UID = "yogass09", PWD = "Qwerty@312",Port = 1433)

# Write data to the SQL database
dbWriteTable(con, "btc_listing_raw", btc_listing_raw, append = TRUE)

# Disconnect from the database
dbDisconnect(con)
