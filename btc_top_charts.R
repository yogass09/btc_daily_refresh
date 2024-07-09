# Record the start time
start_time <- Sys.time()

# Libraries
library(DBI)
library(odbc)
library(dplyr)
library(crypto2)
library(lubrilast_updated)
library(tidyverse)
library(quantmod)

# Set up your Azure SQL Database connection
con <- dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server",
                 Server = "cp-io-sql.database.windows.net", Database = "sql_db_ohlcv",
                 UID = "yogass09", PWD = "Qwerty@312",Port = 1433)


btc_listing_raw <- dbGetQuery(con, paste("SELECT * FROM", "btc_listing_raw"))



## creating df for charting data
btc_top_charts <- btc_listing_raw

                ## Mayers Multiple ## 

btc_top_charts$ma189.price <- zoo::rollmean(btc_top_charts$price, 189, na.pad = TRUE)

# Calculate Mayer's Multiple
btc_top_charts$Mayer_Multiple <- btc_top_charts$price / btc_top_charts$MA365

                ## Puells Multiple ~~

# Assuming your data is in a data frame called 'btc_top_charts'
btc_top_charts <- btc_top_charts %>%
  arrange(last_updated) %>%
  mutate(daily_issuance = c(0, total_supply[-n()] - total_supply[-1])) %>%
  fill(daily_issuance, .direction = "up")

# Calculate 365-day moving average of daily issuance
btc_top_charts <- btc_top_charts %>%
  mutate(issuance.ma = zoo::rollmean(daily_issuance, 365, na.pad = TRUE))

# Calculate Puell Multiple
btc_top_charts <- btc_top_charts %>%
  mutate(puell_multiple = daily_issuance / issuance.ma)


              ## NVT ## 


# Calculate the ratio of market cap to daily transaction volume
btc_top_charts <- btc_top_charts %>% 
  filter(volume24h > 0) %>%
  mutate(mc_tx_ratio =  market_cap/ volume24h)

            ## Draw - Down ##  


btc_top_charts <- btc_top_charts %>%
  mutate(max_price = cummax(price),
         drawdown = (max_price - price) / max_price)

# Calculate the maximum drawdown
max_drawdown <- btc_top_charts %>%
  summarize(max_drawdown = max(drawdown) * 100) %>%
  pull()

          ## Halving Cycles ## 

# Assuming your dataset is named 'btc_top_charts' and the date column is named 'last_updated'
btc_top_charts <- btc_top_charts %>%
  mutate(date = as.Date(last_updated),
         day = day(date),
         month = month(date),
         year = year(date),
         quarter = quarter(date))


# Write data to the SQL database
dbWriteTable(con, "btc_top_charts", btc_top_charts , overwrite = TRUE)

# Disconnect from the database
dbDisconnect(con)


end_time <- Sys.time()

end_time-start_time

