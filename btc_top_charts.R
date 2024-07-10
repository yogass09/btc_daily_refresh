# Record the start time
start_time <- Sys.time()

# Libraries
library(DBI)
library(odbc)
library(dplyr)
library(crypto2)
library(lubridate)
library(tidyverse)
library(quantmod)

# Set up your Azure SQL Database connection
con <- dbConnect(odbc::odbc(), Driver = "ODBC Driver 17 for SQL Server",
                 Server = "cp-io-sql.database.windows.net", Database = "sql_db_ohlcv",
                 UID = "yogass09", PWD = "Qwerty@312",Port = 1433)


btc_listing_raw <- dbGetQuery(con, paste("SELECT * FROM", "btc_listing_raw"))


## data frame switch

btc_top_charts <- btc_listing_raw %>%
  filter(!is.na(circulating_supply) & 
           !is.na(total_supply) & 
           !is.na(max_supply) & 
           !is.na(volume24h)) %>%
  arrange(desc(last_updated))



                ## Mayers Multiple ## 

btc_top_charts$ma200.price <-round( zoo::rollmean(btc_top_charts$price, 200, na.pad = TRUE,align="left"),2 )

# Calculate Mayer's Multiple and round to 2 decimal places
btc_top_charts$Mayer_Multiple <- round(btc_top_charts$price / btc_top_charts$ma200.price, 2)


                ## Puells Multiple ~~

# Assuming your data is in a data frame called 'btc_top_charts'

btc_top_charts <- btc_top_charts %>%
  arrange(last_updated) %>%
  mutate(daily_issuance = na.locf(c(0, diff(total_supply))))

btc_top_charts <- btc_top_charts %>%
  mutate(issuance.ma = zoo::rollmean(daily_issuance, 365, na.pad = TRUE, align = "right"))

btc_top_charts <- btc_top_charts %>%
  mutate(puell_multiple = daily_issuance / issuance.ma)


            ## NVT ## 


# Calculate the ratio of market cap to daily transaction volume
btc_top_charts <- btc_top_charts %>% 
  filter(volume24h > 0) %>%
  mutate(mc_tx_ratio =  round(market_cap/ volume24h),2)


            ## DrawDown ##  


btc_top_charts <- btc_top_charts %>%
  mutate(max_price = cummax(price),
         drawdown = (max_price - price) / max_price)

# Calculate the maximum drawdown
max_drawdown <- btc_top_charts %>%
  summarize(max_drawdown =max(drawdown) * 100) %>%
  pull(max_drawdown)


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

