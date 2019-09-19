
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

read
## 0 - read the currency data file to be used for the conversions

# read the currency tables as dataframe
currency_df <- fread('C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/currency/currency_data_fb_new.csv')
str(currency_df)



## 1- read the full dataset first

# create a vector of allthe curriences to be considered in the dataset
currency_var <- c('MYR', 'SGD', 'TWD', 'IDR', 'THB', 'USD', 'AUD', 'HKD', 'JPY', 'NZD')

# read the dataset into R by providing the file name here

# ---------------------------------- Use when reading from csv -------------------------------#
# facebook_standard_filename <- "facebook_standard_20190825.csv"
# file_date <- '20190825'
# facebook_standard_full <- fread(facebook_standard_filename)
# facebook_standard_full[is.na(facebook_standard_full)] <- 0

# ---------------------------------- Use when reading from xls or xlsx -------------------------------#
facebook_standard_filename <- "facebook_standard_20190908.xlsx"
facebook_standard_output_filename <- "facebook_standard_20190908_transformed.csv"
file_date <- '20190908'
facebook_standard_full <- read_excel(facebook_standard_filename, sheet = 1)
facebook_standard_full[is.na(facebook_standard_full)] <- 0

# subset the extra rows based on the currencies
facebook_standard_full$Advertiser_Currency <- facebook_standard_full$`Currency`
facebook_standard_full.subset <- dplyr::filter(facebook_standard_full, Advertiser_Currency %in% currency_var)

# remove the columns which have no data
facebook_standard_full.subset <- dplyr::select(facebook_standard_full.subset, -starts_with('V'))

# store the full data in the original file name for further processing
facebook_standard <- facebook_standard_full.subset
str(facebook_standard)



## 2- fix the date format for Bigquery upload purposes

# fix the date format of the file
facebook_standard$betterDate <- as.Date(facebook_standard$`Reporting Starts`, format = "%Y-%m-%d")
facebook_standard$newDate <- format(facebook_standard$betterDate, "%Y%m%d")
str(facebook_standard)



## 3- extract the brand and the market name in the dataset

# Extract the market
facebook_standard$key <- paste(facebook_standard$`Campaign Name`, "-", facebook_standard$Account) #Creating key before running string match
facebook_standard$Market.interim <- str_match(facebook_standard$key, "NZ|JP|AU|HK|TW|VN|SG|MY|PH|TH|ID|Indonesia|Malaysia|Thailand|Australia|Japan")
facebook_standard$Market <- str_replace(facebook_standard$Market.interim, " ", "" )

facebook_standard.subset1 <- facebook_standard

facebook_standard.subset1$Market <- case_when(
  facebook_standard.subset1$Currency == "THB" ~ "TH",
  facebook_standard.subset1$Currency == "MYR" ~ "MY",
  facebook_standard.subset1$Currency == "SGD" ~ "SG",
  facebook_standard.subset1$Currency == "TWD" ~ "TW",
  facebook_standard.subset1$Currency == "IDR" ~ "ID",
  facebook_standard.subset1$Currency == "AUD" ~ "AU",
  facebook_standard.subset1$Currency == "HKD" ~ "HK",
  facebook_standard.subset1$Currency == "JPY" ~ "JP",
  facebook_standard.subset1$Currency == "NZD" ~ "NZ",
  facebook_standard.subset1$Currency == "USD" ~ facebook_standard.subset1$Market
)

facebook_standard <- facebook_standard.subset1

# QC if the function worked correctly
table(facebook_standard$Market)
str(facebook_standard)
table(is.na(facebook_standard$Market))

# Extract the brand
facebook_standard$Brand.interim <- str_match(facebook_standard$key, "Physiogel|Sensodyne|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Aquafresh|Poligrip|ProNamel|Contac|Zyrtec|Eno|ENO")
facebook_standard$Brand <- str_replace(facebook_standard$Brand.interim, " ", "-" )

# QC if the function worked correctly
table(facebook_standard$Brand)
str(facebook_standard)
table(is.na(facebook_standard$Brand))


## 4- add in the currency in GBP and local currency

# code block to add currency in GBP and local currency - need to develop

# check the data before joining the two columns
str(currency_df)
str(facebook_standard)

facebook_standard_final <- dplyr::left_join(facebook_standard, currency_df, by = c('Market', 'Advertiser_Currency'))
str(facebook_standard_final)

facebook_standard_final$Spends_Pounds <- facebook_standard_final$`Amount Spent`* facebook_standard_final$Convert_to_Pound
facebook_standard_final$Spends_Local <- facebook_standard_final$`Amount Spent`* facebook_standard_final$Convert_to_Local
str(facebook_standard_final)


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
# older creative (pd)
# size (sz)

### ----------------- Code for extraction from Campaign Name ---------------------------###

# market
facebook_standard_final$MK.interim <- str_match(facebook_standard_final$`Campaign Name`, regex('MK~[^_]*|mk~[^_]*|Mk~[^_]*|mK~[^_]*'))
facebook_standard_final$MK.final <- str_replace(facebook_standard_final$MK.interim, "MK~|mk~|Mk~|mK", "")
facebook_standard_final$MK.final_lower <- tolower(facebook_standard_final$MK.final)

# brand
facebook_standard_final$PR.interim <- str_match(facebook_standard_final$`Campaign Name`, regex('PR~[^_]*|pr~[^_]*|Pr~[^_]*|pR~[^_]*'))
facebook_standard_final$PR.final <- str_replace(facebook_standard_final$PR.interim, "PR~|pr~|Pr~|pR~", "")
facebook_standard_final$PR.final_lower <- tolower(facebook_standard_final$PR.final)

# campaign name
facebook_standard_final$campaign.interim <- str_match(facebook_standard_final$`Campaign Name`, regex('CN~[^_]*|cn~[^_]*'))
facebook_standard_final$campaign.final <- str_replace(facebook_standard_final$campaign.interim, "CN~", "")

# job number
facebook_standard_final$JO.interim <- str_match(facebook_standard_final$`Campaign Name`, regex('JO~[^_]*|jo~[^_]*'))
facebook_standard_final$JO.final <- str_replace(facebook_standard_final$JO.interim, "JO~", "")

# primary kpis
facebook_standard_final$PK.interim <- str_match(facebook_standard_final$`Campaign Name`, regex('PK~[^_]*|pk~[^_]*'))
facebook_standard_final$PK.final <- str_replace(facebook_standard_final$PK.interim, "PK~", "")

### ----------------- Code for extraction from Ad Set Name ---------------------------###

# format
facebook_standard_final$FM.interim <- str_match(facebook_standard_final$`Ad Set Name`, regex('FM~[^_]*|fm~[^_]*'))
facebook_standard_final$FM.final <- str_replace(facebook_standard_final$FM.interim, "FM~", "")
facebook_standard_final$FM.final_lower <- tolower(facebook_standard_final$FM.final)

# objective
facebook_standard_final$OB.interim <- str_match(facebook_standard_final$`Ad Set Name`, regex('OB~[^_]*|ob~[^_]*'))
facebook_standard_final$OB.final <- str_replace(facebook_standard_final$OB.interim, "OB~", "")
facebook_standard_final$OB.final_lower <- tolower(facebook_standard_final$OB.final)

# device type
facebook_standard_final$DT.interim <- str_match(facebook_standard_final$`Ad Set Name`, regex('DT~[^_]*|dt~[^_]*'))
facebook_standard_final$DT.final <- str_replace(facebook_standard_final$DT.interim, "DT~", "")
facebook_standard_final$DT.final_lower <- tolower(facebook_standard_final$DT.final)

# publisher
facebook_standard_final$PB.interim <- str_match(facebook_standard_final$`Ad Set Name`, regex('PB~[^_]*|pb~[^_]*'))
facebook_standard_final$PB.final <- str_replace(facebook_standard_final$PB.interim, "PB~", "")
facebook_standard_final$PB.final_lower <- tolower(facebook_standard_final$PB.final)

# adset name
facebook_standard_final$SA.interim <- str_match(facebook_standard_final$`Ad Set Name`, regex('SA~[^_]*|sa~[^_]*'))
facebook_standard_final$SA.final <- str_replace(facebook_standard_final$SA.interim, "SA~", "")
facebook_standard_final$SA.final_lower <- tolower(facebook_standard_final$SA.final)

### ----------------- Code for extraction from Ad Name ---------------------------###

# creative
facebook_standard_final$FF.interim <- str_match(facebook_standard_final$`Ad Name`, regex('FF~[^_]*|ff~[^_]*'))
facebook_standard_final$FF.final <- str_replace(facebook_standard_final$FF.interim, "FF~", "")

# pd - older field for creative
facebook_standard_final$PD.interim <- str_match(facebook_standard_final$`Ad Name`, regex('PD~[^_]*|pd~[^_]*'))
facebook_standard_final$PD.final <- str_replace(facebook_standard_final$PD.interim, "PD~", "")

# sz - creative size
facebook_standard_final$SZ.interim <- str_match(facebook_standard_final$`Ad Name`, regex('SZ~[^_]*|sz~[^_]*'))
facebook_standard_final$SZ.final <- str_replace(facebook_standard_final$SZ.interim, "SZ~", "")

# QC after field extraction
str(facebook_standard_final)
# write.csv(facebook_standard_final, "facebook_test.csv")



## 6- removing additional fields
facebook_standard_final <- select(facebook_standard_final, -Convert_to_Pound, -Convert_to_Local, -newDate)
facebook_standard_final <- select(facebook_standard_final, -contains("interim"))



## 7- adding the description of the taxonomy

# read the mapping table and left join with the base table

### ------------------------ Campaign level names ----------------------- ###

# MK
mapping_df_mk <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 8, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_mk, by=c("MK.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, MK.Description = Description)

# PR
mapping_df_pr <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 9, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_pr, by=c("PR.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, PR.Description = Description)


### ----------------------- Ad set level codes --------------------------- ###

# FM
mapping_df_fm <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 1, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_fm, by=c("FM.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, FM.Description = Description)

# OB
mapping_df_ob <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 2, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_ob, by=c("OB.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, OB.Description = Description)

# DT
mapping_df_dt <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 6, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_dt, by=c("DT.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, DT.Description = Description)

# PB
mapping_df_pb <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 10, col_names = T, col_types = 'text')
facebook_standard_final <- left_join(facebook_standard_final, mapping_df_pb, by=c("PB.final_lower" = "Code_lower"))
facebook_standard_final <- rename(facebook_standard_final, PB.Description = Description)

# QC
str(facebook_standard_final)
write.csv(facebook_standard_final, facebook_standard_output_filename, row.names = F, fileEncoding = "UTF-8")

