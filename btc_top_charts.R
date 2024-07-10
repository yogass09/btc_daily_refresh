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

btc_top_charts <- btc_listing_raw[order(btc_listing_raw$last_updated, decreasing = TRUE), ]


                ## Mayers Multiple ## 

btc_top_charts$ma200.price <- zoo::rollmean(btc_top_charts$price, 200, na.pad = TRUE,align="left")

# Calculate Mayer's Multiple
btc_top_charts$Mayer_Multiple <- btc_top_charts$price / btc_top_charts$ma200.price


library(ggplot2)

# Plot Mayer's Multiple
#ggplot(btc_top_charts, aes(x = last_updated, y = Mayer_Multiple)) +
 # geom_line(color = "green", size = 1) +
 # labs(title = "Mayer's Multiple Over Time",
 #      x = "Date",
 #      y = "Mayer's Multiple") +
 # theme_minimal() +
 # theme(axis.text.x = element_text(angle = 90, hjust = 5))


                ## Puells Multiple ~~

# Assuming your data is in a data frame called 'btc_top_charts'
btc_top_charts <- btc_top_charts %>%
  arrange(last_updated) %>%
  mutate(daily_issuance = c(0, diff(total_supply))) %>%
  na.locf()

# Calculate 365-day moving average of daily issuance
btc_top_charts <- btc_top_charts %>%
  mutate(issuance.ma = zoo::rollmean(daily_issuance, 365, na.pad = TRUE,align = "right"))

# Calculate Puell Multiple
btc_top_charts <- btc_top_charts %>%
  mutate(puell_multiple = daily_issuance / issuance.ma)


# Plotting Puell Multiple and issuance.ma against price
library(ggplot2)

# Plot Puell's Multiple with log scale and limited y-axis range
#ggplot(btc_top_charts, aes(x = last_updated, y = puell_multiple)) +
 # geom_line(color = "blue", size = 1) +
  #labs(title = "Puell's Multiple Over Time",
   #    x = "Date",
    #   y = "Puell's Multiple") +
  #scale_y_continuous(trans = "log10", limits = c(0.1, 10), breaks = c(0.1, 1, 10)) +
  #theme_minimal() +
  #theme(axis.text.x = element_text(angle = 45, hjust = 1))


              ## NVT ## 


# Calculate the ratio of market cap to daily transaction volume
btc_top_charts <- btc_top_charts %>% 
  filter(volume24h > 0) %>%
  mutate(mc_tx_ratio =  market_cap/ volume24h)

library(ggplot2)

# Plot mc_tx_ratio with log scale and limited y-axis range
#ggplot(btc_top_charts, aes(x = last_updated, y = mc_tx_ratio)) +
 # geom_line(color = "purple", size = 1) +
  #scale_y_continuous(trans = "log10", limits = c(0.01, 1000), breaks = c(0.01, 0.1, 1, 10, 100, 1000)) +
 # labs(title = "Market Cap to 24h Volume Ratio Over Time",
 #      x = "Date",
 #      y = "Market Cap to Volume Ratio (Log Scale)") +
 # theme_minimal() +
 # theme(axis.text.x = element_text(angle = 45, hjust = 1))


            ## DrawDown ##  

library(tidyverse)


btc_top_charts <- btc_top_charts %>%
  mutate(max_price = cummax(price),
         drawdown = (max_price - price) / max_price)

# Calculate the maximum drawdown
max_drawdown <- btc_top_charts %>%
  summarize(max_drawdown = max(drawdown) * 100) %>%
  pull()


#library(ggplot2)

# Plot maximum drawdown
#ggplot(btc_top_charts, aes(x = last_updated, y = drawdown)) +
 # geom_line(color = "red", size = 1) +
  #labs(title = "Maximum Drawdown Over Time",
   #    x = "Date",
    #   y = "Drawdown (%)") +
 # theme_minimal() +
 # theme(axis.text.x = element_text(angle = 45, hjust = 1))


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

