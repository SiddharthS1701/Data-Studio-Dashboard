
# set the working directory
working_location <- 'C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/new working directory/input'
setwd(working_location)
getwd()

# install the packages
# install.packages('stringr')
# install.packages('bigQueryR')
# install.packages('sqldf')
# install.packages('xlsx')
# install.packages('googleCloudStorageR')

# load the libaries
library(stringr)
library(data.table)
library(dplyr)
library(sqldf)
library(xlsx)
library(readxl)
# library(googleCloudStorageR)


## 0 - read the currency data file to be used for the conversions

# read the currency tables as dataframe
currency_df <- fread('C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/currency/currency_data_fb_new.csv')
str(currency_df)



## 1- read the full dataset first

# create a vector of allthe curriences to be considered in the dataset
currency_var <- c('MYR', 'SGD', 'TWD', 'IDR', 'THB', 'USD', 'AUD', 'HKD', 'JPY', 'NZD')

# read the dataset into R by providing the file name here

# ---------------------------------- Use when reading from csv -------------------------------#
# facebook_reach_filename <- "facebook_reach_20190825.csv"
# facebook_reach_output_filename <- "facebook_reach_20190825.csv"
# file_date <- '20190825'
# facebook_reach_full <- fread(facebook_reach_filename)
# facebook_reach_full[is.na(facebook_reach_full)] <- 0

# ---------------------------------- Use when reading from xls or xlsx -------------------------------#
facebook_reach_filename <- "facebook_reach_20190908.xlsx"
facebook_reach_output_filename <- "facebook_reach_20190908_transformed.csv"
file_date <- '20190908'
facebook_reach_full <- read_excel(facebook_reach_filename, sheet = 1)
facebook_reach_full[is.na(facebook_reach_full)] <- 0

# subset the extra rows based on the currencies
facebook_reach_full$Advertiser_Currency <- facebook_reach_full$`Currency`
facebook_reach_full.subset <- dplyr::filter(facebook_reach_full, Advertiser_Currency %in% currency_var)

# remove the columns which have no data
facebook_reach_full.subset <- dplyr::select(facebook_reach_full.subset, -starts_with('V'))

# store the full data in the original file name for further processing
facebook_reach <- facebook_reach_full.subset
str(facebook_reach)



## 2- fix the date format for Bigquery upload purposes

## -------------- for reach view in data studio, disable the date columns in data studio ------------- ##
# fix the date format of the file
facebook_reach$betterDate <- as.Date(facebook_reach$`Reporting Starts`, format = "%Y-%m-%d")
facebook_reach$newDate <- format(facebook_reach$betterDate, "%Y%m%d")
str(facebook_reach)



## 3- extract the brand and the market name in the dataset

# Extract the market
facebook_reach$key <- paste(facebook_reach$`Campaign Name`, "-", facebook_reach$Account) #Creating key before running string match
facebook_reach$Market.interim <- str_match(facebook_reach$key, "NZ|JP|AU|HK|TW|VN|SG|MY|PH|TH|ID|Indonesia|Malaysia|Thailand|Australia|Japan")
facebook_reach$Market <- str_replace(facebook_reach$Market.interim, " ", "" )

facebook_reach.subset1 <- facebook_reach

facebook_reach.subset1$Market <- case_when(
  facebook_reach.subset1$Currency == "THB" ~ "TH",
  facebook_reach.subset1$Currency == "MYR" ~ "MY",
  facebook_reach.subset1$Currency == "SGD" ~ "SG",
  facebook_reach.subset1$Currency == "TWD" ~ "TW",
  facebook_reach.subset1$Currency == "IDR" ~ "ID",
  facebook_reach.subset1$Currency == "AUD" ~ "AU",
  facebook_reach.subset1$Currency == "HKD" ~ "HK",
  facebook_reach.subset1$Currency == "JPY" ~ "JP",
  facebook_reach.subset1$Currency == "NZD" ~ "NZ",
  facebook_reach.subset1$Currency == "USD" ~ facebook_reach.subset1$Market
)

facebook_reach <- facebook_reach.subset1

# QC if the function worked correctly
table(facebook_reach$Market)
str(facebook_reach)
table(is.na(facebook_reach$Market))

# Extract the brand
facebook_reach$Brand.interim <- str_match(facebook_reach$key, "Physiogel|Sensodyne|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Aquafresh|Poligrip|ProNamel|Contac|Zyrtec|Eno|ENO")
facebook_reach$Brand <- str_replace(facebook_reach$Brand.interim, " ", "-" )

# QC if the function worked correctly
table(facebook_reach$Brand)
str(facebook_reach)
table(is.na(facebook_reach$Brand))


## 4- add in the currency in GBP and local currency

# code block to add currency in GBP and local currency - need to develop

# check the data before joining the two columns
str(currency_df)
str(facebook_reach)

facebook_reach_final <- dplyr::left_join(facebook_reach, currency_df, by = c('Market', 'Advertiser_Currency'))
str(facebook_reach_final)

facebook_reach_final$Spends_Pounds <- facebook_reach_final$`Amount Spent`* facebook_reach_final$Convert_to_Pound
facebook_reach_final$Spends_Local <- facebook_reach_final$`Amount Spent`* facebook_reach_final$Convert_to_Local
str(facebook_reach_final)


## 5- extract the following fields from the new taxonomy fields

# taxonomy status

### ----------------- Extract from campaign name -------------------------###
# market (mk)
# brand (pr)
# campaign name (cn)
# job number (jo)


### ------------- Code for extraction from Campaign Name -----------------###

# market
facebook_reach_final$MK.interim <- str_match(facebook_reach_final$`Campaign Name`, regex('MK~[^_]*|mk~[^_]*|Mk~[^_]*|mK~[^_]*'))
facebook_reach_final$MK.final <- str_replace(facebook_reach_final$MK.interim, "MK~|mk~|Mk~|mK", "")
facebook_reach_final$MK.final_lower <- tolower(facebook_reach_final$MK.final)

# brand
facebook_reach_final$PR.interim <- str_match(facebook_reach_final$`Campaign Name`, regex('PR~[^_]*|pr~[^_]*|Pr~[^_]*|pR~[^_]*'))
facebook_reach_final$PR.final <- str_replace(facebook_reach_final$PR.interim, "PR~|pr~|Pr~|pR~", "")
facebook_reach_final$PR.final_lower <- tolower(facebook_reach_final$PR.final)

# campaign name
facebook_reach_final$campaign.interim <- str_match(facebook_reach_final$`Campaign Name`, regex('CN~[^_]*|cn~[^_]*'))
facebook_reach_final$campaign.final <- str_replace(facebook_reach_final$campaign.interim, "CN~", "")

# job number
facebook_reach_final$JO.interim <- str_match(facebook_reach_final$`Campaign Name`, regex('JO~[^_]*|jo~[^_]*'))
facebook_reach_final$JO.final <- str_replace(facebook_reach_final$JO.interim, "JO~", "")

# QC after field extraction
str(facebook_reach_final)
# write.csv(facebook_reach_final, "facebook_test.csv")



## 6- removing additional fields
facebook_reach_final <- select(facebook_reach_final, -Convert_to_Pound, -Convert_to_Local, -newDate)
facebook_reach_final <- select(facebook_reach_final, -contains("interim"))



## 7- adding the description of the taxonomy

# read the mapping table and left join with the base table

### ------------------------ Campaign level names ----------------------- ###

# MK
mapping_df_mk <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 8, col_names = T, col_types = 'text')
facebook_reach_final <- left_join(facebook_reach_final, mapping_df_mk, by=c("MK.final_lower" = "Code_lower"))
facebook_reach_final <- rename(facebook_reach_final, MK.Description = Description)

# PR
mapping_df_pr <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 9, col_names = T, col_types = 'text')
facebook_reach_final <- left_join(facebook_reach_final, mapping_df_pr, by=c("PR.final_lower" = "Code_lower"))
facebook_reach_final <- rename(facebook_reach_final, PR.Description = Description)



## 8 - QC and write the dataset to csv for export to BigQuery
str(facebook_reach_final)
write.csv(facebook_reach_final, facebook_reach_output_filename, row.names = F, fileEncoding = "UTF-8")

