
# set the working directory
working_location <- 'C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/new working directory/input'
setwd(working_location)
getwd()

# ----------------------- install the packages --------------------------- #
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
#library(googleCloudStorageR)

# read the currency tables as dataframe
currency_df <- fread('C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/currency/currency_data_underscore.csv')
str(currency_df)

## 1- read the full dataset first

# create a vector of allthe curriences to be considered in the dataset
currency_var <- c('MYR', 'SGD', 'TWD', 'IDR', 'THB', 'USD', 'AUD', 'HKD', 'JPY', 'NZD')

# read the dataset into R by providing the file name here
dv360_standard_filename <- "dv360_standard_20190908.csv"
file_date <- '20190908'

# read the file
dv360_standard_full <- read.csv(dv360_standard_filename, header = T, stringsAsFactors = F, check.names = F, encoding = 'UTF-8')

# subset the extra rows based on the currencies
dv360_standard_full$Advertiser_Currency <- dv360_standard_full$`Advertiser Currency`
dv360_standard_full.subset <- dplyr::filter(dv360_standard_full, Advertiser_Currency %in% currency_var)

# remove the columns which have no data
dv360_standard_full.subset <- dplyr::select(dv360_standard_full.subset, -starts_with('V'))

# store the full data in the original file name for further processing
dv360_standard <- dv360_standard_full.subset
str(dv360_standard)


## 2- fix the date format for Bigquery upload purposes

# fix the date format of the file
dv360_standard$betterDate <- as.Date(dv360_standard$Date, format = "%Y/%m/%d")
#dv360_standard$betterDate <- as.Date(dv360_standard$Date, format = "%m/%d/%Y")
dv360_standard$newDate <- format(dv360_standard$betterDate, "%Y%m%d")
str(dv360_standard)


## 3- extract the brand and the market name in the dataset

# Extract the market
dv360_standard$key <- paste(dv360_standard$Campaign, "-", dv360_standard$Advertiser) #Creating key before running string match
dv360_standard$Market.interim <- str_match(dv360_standard$key, "NZ|JP|AU|HK|TW|VN|SG|MY|PH|TH|ID|Indonesia|Malaysia|Thailand|Australia|Japan")
dv360_standard$Market <- str_replace(dv360_standard$Market.interim, " ", "" )

# QC if the function worked correctly
table(dv360_standard$Market)
str(dv360_standard)
table(is.na(dv360_standard$Market))

# Extract the brand
dv360_standard$Brand.interim <- str_match(dv360_standard$key, "Physiogel|Sensodyne|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Aquafresh|Poligrip|ProNamel|Contac|Zyrtec|Eno|ENO")
dv360_standard$Brand <- str_replace(dv360_standard$Brand.interim, " ", "-" )

# QC if the function worked correctly
table(dv360_standard$Brand)
str(dv360_standard)
table(is.na(dv360_standard$Brand))


## 4- add in the currency in GBP and local currency

# adding currency in pounds and local
# check the data before joining the two columns
str(currency_df)
str(dv360_standard)

dv360_standard_final <- dplyr::left_join(dv360_standard, currency_df, by = c('Market', 'Advertiser_Currency'))
str(dv360_standard_final)

dv360_standard_final$Spends_Pounds <- dv360_standard_final$`Revenue (Adv Currency)`* dv360_standard_final$Convert_to_Pound
dv360_standard_final$Spends_Local <- dv360_standard_final$`Revenue (Adv Currency)`* dv360_standard_final$Convert_to_Local


## 5- extract the following fields from the new taxonomy fields

# ------------------------ Extract at a Campaign Name level-------------------- #
# market
# brand
# campaign name (cn)
# taxonomy status

# ------------------------ Extract at a Insertion Order level------------------ #
# primary KPIs (pk)
# job number (jo)
# format (fm)
# objective (ob)
# targeting (tg)
# targeting sub type (ts)
# creative (ff)

# ------------------------ Code to extract data at a Campaign Name level-------------------- #
# market
dv360_standard_final$MK.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('MK~[^_]*|mk~[^_]*|Mk~[^_]*|mK~[^_]*'))
dv360_standard_final$MK.final <- str_replace(dv360_standard_final$MK.interim, "MK~|mk~|Mk~|mK", "")
dv360_standard_final$MK.final_lower <- tolower(dv360_standard_final$MK.final)

# brand
dv360_standard_final$PR.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('PR~[^_]*|pr~[^_]*|Pr~[^_]*|pR~[^_]*'))
dv360_standard_final$PR.final <- str_replace(dv360_standard_final$PR.interim, "PR~|pr~|Pr~|pR~", "")
dv360_standard_final$PR.final_lower <- tolower(dv360_standard_final$PR.final)

# campaign name
dv360_standard_final$campaign.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('CN~[^_]*|cn~[^_]*|cN~[^_]*|Cn~[^_]*'))
dv360_standard_final$campaign.final <- str_replace(dv360_standard_final$campaign.interim, "CN~|cn~|cN~|Cn~", "")

# taxonomy status
dv360_standard_final <- dplyr::mutate(dv360_standard_final, taxonomy_status = ifelse(is.na(campaign.final), "old", "new"))
table(dv360_standard_final$taxonomy_status)

# ---------------------- Code to Extract information at a Insertion Order level ------------------------#
# primary kpis
dv360_standard_final$PK.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('PK~[^_]*|pk~[^_]*|Pk~[^_]*|pK~[^_]*'))
dv360_standard_final$PK.final <- str_replace(dv360_standard_final$PK.interim, "PK~|pk~|pK~|Pk~", "")

# job number
dv360_standard_final$JO.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('JO~[^_]*|jo~[^_]*|jO~[^_]*|Jo~[^_]*'))
dv360_standard_final$JO.final <- str_replace(dv360_standard_final$JO.interim, "JO~|jo~|Jo~|jO~", "")

# format
dv360_standard_final$FM.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('FM~[^_]*|fm~[^_]*|fM~[^_]*|Fm~[^_]*'))
dv360_standard_final$FM.final <- str_replace(dv360_standard_final$FM.interim, "FM~|fm~|fM~|Fm~", "")
dv360_standard_final$FM.final_lower <- tolower(dv360_standard_final$FM.final)

# objective
dv360_standard_final$OB.interim <- str_match(dv360_standard_final$`Insertion Order`, regex('OB~[^_]*|ob~[^_]*|oB~[^_]*|Ob~[^_]*'))
dv360_standard_final$OB.final <- str_replace(dv360_standard_final$OB.interim, "OB~|ob~|oB~|Ob~", "")
dv360_standard_final$OB.final_lower <- tolower(dv360_standard_final$OB.final)

# targeting
dv360_standard_final$TG.interim <- str_match(dv360_standard_final$`Line Item`, regex('TG~[^_]*|tg~[^_]*|tG~[^_]*|Tg~[^_]*'))
dv360_standard_final$TG.final <- str_replace(dv360_standard_final$TG.interim, "TG~|tg~|tG~|Tg~", "")
dv360_standard_final$TG.final_lower <- tolower(dv360_standard_final$TG.final)

# targeting sub type
dv360_standard_final$TS.interim <- str_match(dv360_standard_final$`Line Item`, regex('TS~[^_]*|ts~[^_]*|Ts~[^_]*|tS~[^_]*'))
dv360_standard_final$TS.final <- str_replace(dv360_standard_final$TS.interim, "TS~|ts~|Ts~|tS~|", "")

# channel
dv360_standard_final$CH.interim <- str_match(dv360_standard_final$`Line Item`, regex('CH~[^_]*|ch~[^_]*|Ch~[^_]*|cH~[^_]*'))
dv360_standard_final$CH.final <- str_replace(dv360_standard_final$CH.interim, "CH~|ch~|cH~|Ch~|", "")
dv360_standard_final$CH.final_lower <- tolower(dv360_standard_final$CH.final)

# device type
dv360_standard_final$DT.interim <- str_match(dv360_standard_final$`Line Item`, regex('DT~[^_]*|dt~[^_]*|dT~[^_]*|Dt~[^_]*'))
dv360_standard_final$DT.final <- str_replace(dv360_standard_final$DT.interim, "DT~|dt~|dT~|Dt~", "")
dv360_standard_final$DT.final_lower <- tolower(dv360_standard_final$DT.final)

# creative 
dv360_standard_final$CR.interim <- str_match(dv360_standard_final$Creative, regex('FF~[^_]*|ff~[^_]*|fF~[^_]*|Ff~[^_]*'))
dv360_standard_final$CR.final <- str_replace(dv360_standard_final$CR.interim, "FF~|ff~|Ff~|fF~", "")

# QC after field extraction
str(dv360_standard_final)

## 6- removing additional fields
dv360_standard_final <- select(dv360_standard_final, -Convert_to_Pound, -Convert_to_Local, -newDate)
dv360_standard_final <- select(dv360_standard_final, -contains("interim"))
dv360_standard_final$Brand_new <- dv360_standard_final$Brand



## 7- adding the description of the taxonomy

# read the mapping table and left join with the base table
# MK
mapping_df_mk <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 8, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_mk, by=c("MK.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, MK.Description = Description)

# PR
mapping_df_pr <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 9, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_pr, by=c("PR.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, PR.Description = Description)

# FM
mapping_df_fm <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 1, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_fm, by=c("FM.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, FM.Description = Description)

# OB
mapping_df_ob <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 2, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_ob, by=c("OB.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, OB.Description = Description)

# TG
mapping_df_tg <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 3, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_tg, by=c("TG.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, TG.Description = Description)

# CH
mapping_df_ch <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 5, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_ch, by=c("CH.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, CH.Description = Description)

# DT
mapping_df_dt <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 6, col_names = T, col_types = 'text')
dv360_standard_final <- left_join(dv360_standard_final, mapping_df_dt, by=c("DT.final_lower" = "Code_lower"))
dv360_standard_final <- rename(dv360_standard_final, DT.Description = Description)

## 8- write back the output
outputfileName <- paste0( "dv360_standard_", file_date, "_transformed.csv")
write.csv(dv360_standard_final, outputfileName, row.names=FALSE, fileEncoding = 'UTF-8')
