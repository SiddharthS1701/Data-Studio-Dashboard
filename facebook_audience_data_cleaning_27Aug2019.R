
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
# facebook_audience_filename <- "facebook_audience_20190901.csv"
# facebook_audience_output_filename <- "facebook_audience_20190901_transformed.csv"
# file_date <- '20190901'
# facebook_audience_full <- fread(facebook_audience_filename)
# facebook_audience_full <- read.csv(file = facebook_audience_filename, header = T, stringsAsFactors = F, check.names = F, encoding = "UTF-8")
# facebook_audience_full[is.na(facebook_audience_full)] <- 0

# ---------------------------------- Use when reading from xls or xlsx -------------------------------#
facebook_audience_filename <- "facebook_audience_20190908.xlsx"
facebook_audience_output_filename <- "facebook_audience_20190908_transformed.csv"
file_date <- '20190908'
facebook_audience_full <- read_excel(facebook_audience_filename, sheet = 1)
facebook_audience_full[is.na(facebook_audience_full)] <- 0

# subset the extra rows based on the currencies
facebook_audience_full$Advertiser_Currency <- facebook_audience_full$`Currency`
facebook_audience_full.subset <- dplyr::filter(facebook_audience_full, Advertiser_Currency %in% currency_var)

# remove the columns which have no data
facebook_audience_full.subset <- dplyr::select(facebook_audience_full.subset, -starts_with('V'))

# store the full data in the original file name for further processing
facebook_audience <- facebook_audience_full.subset
str(facebook_audience)



## 2- fix the date format for Bigquery upload purposes

# fix the date format of the file
facebook_audience$betterDate <- as.Date(facebook_audience$`Reporting Starts`, format = "%Y-%m-%d")
facebook_audience$newDate <- format(facebook_audience$betterDate, "%Y%m%d")
str(facebook_audience)



## 3- extract the brand and the market name in the dataset

# Extract the market
facebook_audience$key <- paste(facebook_audience$`Campaign Name`, "-", facebook_audience$Account) #Creating key before running string match
facebook_audience$Market.interim <- str_match(facebook_audience$key, "NZ|JP|AU|HK|TW|VN|SG|MY|PH|TH|ID|Indonesia|Malaysia|Thailand|Australia|Japan")
facebook_audience$Market <- str_replace(facebook_audience$Market.interim, " ", "" )

facebook_audience.subset1 <- facebook_audience

facebook_audience.subset1$Market <- case_when(
  facebook_audience.subset1$Currency == "THB" ~ "TH",
  facebook_audience.subset1$Currency == "MYR" ~ "MY",
  facebook_audience.subset1$Currency == "SGD" ~ "SG",
  facebook_audience.subset1$Currency == "TWD" ~ "TW",
  facebook_audience.subset1$Currency == "IDR" ~ "ID",
  facebook_audience.subset1$Currency == "AUD" ~ "AU",
  facebook_audience.subset1$Currency == "HKD" ~ "HK",
  facebook_audience.subset1$Currency == "JPY" ~ "JP",
  facebook_audience.subset1$Currency == "NZD" ~ "NZ",
  facebook_audience.subset1$Currency == "USD" ~ facebook_audience.subset1$Market
)

facebook_audience <- facebook_audience.subset1

# QC if the function worked correctly
table(facebook_audience$Market)
str(facebook_audience)
table(is.na(facebook_audience$Market))

# Extract the brand
facebook_audience$Brand.interim <- str_match(facebook_audience$key, "Physiogel|Sensodyne|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Aquafresh|Poligrip|ProNamel|Contac|Zyrtec|Eno|ENO")
facebook_audience$Brand <- str_replace(facebook_audience$Brand.interim, " ", "-" )

# QC if the function worked correctly
table(facebook_audience$Brand)
str(facebook_audience)
table(is.na(facebook_audience$Brand))


## 4- add in the currency in GBP and local currency

# code block to add currency in GBP and local currency - need to develop

# check the data before joining the two columns
str(currency_df)
str(facebook_audience)

facebook_audience_final <- dplyr::left_join(facebook_audience, currency_df, by = c('Market', 'Advertiser_Currency'))
str(facebook_audience_final)

facebook_audience_final$Spends_Pounds <- facebook_audience_final$`Amount Spent`* facebook_audience_final$Convert_to_Pound
facebook_audience_final$Spends_Local <- facebook_audience_final$`Amount Spent`* facebook_audience_final$Convert_to_Local
str(facebook_audience_final)


## 5- extract the following fields from the new taxonomy fields

# taxonomy status

### ----------------- Extract from campaign name -------------------------###
# market (mk)
# brand (pr)
# campaign name (cn)
# job number (jo)
# primary kpis (pk)

### ----------------- Extract from Ad set name ---------------------------###
# format (fm) 
# objective (ob)
# device type (dt)
# publisher (pb)
# audience segment (sa)

### ----------------- Extract from Ad name ---------------------------###
# creative (ff)
# older creative name (pd)
# size (sz)

### ----------------- Code for extraction from Campaign Name ---------------------------###

# market
facebook_audience_final$MK.interim <- str_match(facebook_audience_final$`Campaign Name`, regex('MK~[^_]*|mk~[^_]*|Mk~[^_]*|mK~[^_]*'))
facebook_audience_final$MK.final <- str_replace(facebook_audience_final$MK.interim, "MK~|mk~|Mk~|mK", "")
facebook_audience_final$MK.final_lower <- tolower(facebook_audience_final$MK.final)

# brand
facebook_audience_final$PR.interim <- str_match(facebook_audience_final$`Campaign Name`, regex('PR~[^_]*|pr~[^_]*|Pr~[^_]*|pR~[^_]*'))
facebook_audience_final$PR.final <- str_replace(facebook_audience_final$PR.interim, "PR~|pr~|Pr~|pR~", "")
facebook_audience_final$PR.final_lower <- tolower(facebook_audience_final$PR.final)

# campaign name
facebook_audience_final$campaign.interim <- str_match(facebook_audience_final$`Campaign Name`, regex('CN~[^_]*|cn~[^_]*'))
facebook_audience_final$campaign.final <- str_replace(facebook_audience_final$campaign.interim, "CN~", "")

# job number
facebook_audience_final$JO.interim <- str_match(facebook_audience_final$`Campaign Name`, regex('JO~[^_]*|jo~[^_]*'))
facebook_audience_final$JO.final <- str_replace(facebook_audience_final$JO.interim, "JO~", "")

# primary kpis
facebook_audience_final$PK.interim <- str_match(facebook_audience_final$`Campaign Name`, regex('PK~[^_]*|pk~[^_]*'))
facebook_audience_final$PK.final <- str_replace(facebook_audience_final$PK.interim, "PK~", "")

### ----------------- Code for extraction from Ad Set Name ---------------------------###

# format
facebook_audience_final$FM.interim <- str_match(facebook_audience_final$`Ad Set Name`, regex('FM~[^_]*|fm~[^_]*'))
facebook_audience_final$FM.final <- str_replace(facebook_audience_final$FM.interim, "FM~", "")
facebook_audience_final$FM.final_lower <- tolower(facebook_audience_final$FM.final)

# objective
facebook_audience_final$OB.interim <- str_match(facebook_audience_final$`Ad Set Name`, regex('OB~[^_]*|ob~[^_]*'))
facebook_audience_final$OB.final <- str_replace(facebook_audience_final$OB.interim, "OB~", "")
facebook_audience_final$OB.final_lower <- tolower(facebook_audience_final$OB.final)

# device type
facebook_audience_final$DT.interim <- str_match(facebook_audience_final$`Ad Set Name`, regex('DT~[^_]*|dt~[^_]*'))
facebook_audience_final$DT.final <- str_replace(facebook_audience_final$DT.interim, "DT~", "")
facebook_audience_final$DT.final_lower <- tolower(facebook_audience_final$DT.final)

# publisher
facebook_audience_final$PB.interim <- str_match(facebook_audience_final$`Ad Set Name`, regex('PB~[^_]*|pb~[^_]*'))
facebook_audience_final$PB.final <- str_replace(facebook_audience_final$PB.interim, "PB~", "")
facebook_audience_final$PB.final_lower <- tolower(facebook_audience_final$PB.final)

# ad set name
facebook_audience_final$SA.interim <- str_match(facebook_audience_final$`Ad Set Name`, regex('SA~[^_]*|sa~[^_]*'))
facebook_audience_final$SA.final <- str_replace(facebook_audience_final$SA.interim, "SA~", "")
facebook_audience_final$SA.final_lower <- tolower(facebook_audience_final$SA.final)

### ----------------- Code for extraction from Ad Name ---------------------------###

# creative
facebook_audience_final$FF.interim <- str_match(facebook_audience_final$`Ad Name`, regex('FF~[^_]*|ff~[^_]*'))
facebook_audience_final$FF.final <- str_replace(facebook_audience_final$FF.interim, "FF~", "")

# pd - older field for creative
facebook_audience_final$PD.interim <- str_match(facebook_audience_final$`Ad Name`, regex('PD~[^_]*|pd~[^_]*'))
facebook_audience_final$PD.final <- str_replace(facebook_audience_final$PD.interim, "PD~", "")

# sz - creative size
facebook_audience_final$SZ.interim <- str_match(facebook_audience_final$`Ad Name`, regex('SZ~[^_]*|sz~[^_]*'))
facebook_audience_final$SZ.final <- str_replace(facebook_audience_final$SZ.interim, "SZ~", "")

# QC after field extraction
str(facebook_audience_final)
# write.csv(facebook_audience_final, "facebook_test.csv")



## 6- removing additional fields
facebook_audience_final <- select(facebook_audience_final, -Convert_to_Pound, -Convert_to_Local, -newDate)
facebook_audience_final <- select(facebook_audience_final, -contains("interim"))



## 7- adding the description of the taxonomy

# read the mapping table and left join with the base table

### ------------------------ Campaign level names ----------------------- ###

# MK
mapping_df_mk <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 8, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_mk, by=c("MK.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, MK.Description = Description)

# PR
mapping_df_pr <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 9, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_pr, by=c("PR.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, PR.Description = Description)


### ----------------------- Ad set level codes --------------------------- ###

# FM
mapping_df_fm <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 1, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_fm, by=c("FM.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, FM.Description = Description)

# OB
mapping_df_ob <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 2, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_ob, by=c("OB.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, OB.Description = Description)

# DT
mapping_df_dt <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 6, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_dt, by=c("DT.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, DT.Description = Description)

# PB
mapping_df_pb <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 10, col_names = T, col_types = 'text')
facebook_audience_final <- left_join(facebook_audience_final, mapping_df_pb, by=c("PB.final_lower" = "Code_lower"))
facebook_audience_final <- rename(facebook_audience_final, PB.Description = Description)

## 8 - Fix the FF and pd issue
facebook_audience_final$FF.final_1 <- ifelse(is.na(facebook_audience_final$FF.final), facebook_audience_final$PD.final, facebook_audience_final$FF.final)

# QC and write the file back to output csv
str(facebook_audience_final)
write.csv(facebook_audience_final, facebook_audience_output_filename, row.names = F)
