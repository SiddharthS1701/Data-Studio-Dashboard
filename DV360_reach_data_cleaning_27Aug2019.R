
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
dv360_reach_filename <- "dv360_reach_20190908.csv"
file_date <- '20190908'

dv360_reach_full <- read.csv(dv360_reach_filename, header = T, stringsAsFactors = F, check.names = F, encoding = "UTF-8")

# subset the extra rows based on the partner
# dv360_reach_full.subset <- dplyr::filter(dv360_reach_full, grepl('GSK CHC', dv360_reach_full$Partner))
 dv360_reach_full.subset <- dv360_reach_full

# remove the columns which have no data
dv360_reach_full.subset <- dplyr::select(dv360_reach_full.subset, -starts_with('V'))

# store the full data in the original file name for further processing
dv360_reach <- dv360_reach_full.subset
str(dv360_reach)

## 2 - Extract and reformat the date for bigquery purposes - Not needed

## 3- extract the brand and the market name in the dataset

# Extract the market
dv360_reach$key <- paste(dv360_reach$`Insertion Order`, "-", dv360_reach$Advertiser) #Creating key before running string match
dv360_reach$Market.interim <- str_match(dv360_reach$key, "NZ|JP|AU|HK|TW|VN|SG|MY|PH|TH|ID|Indonesia|Malaysia|Thailand|Australia|Japan")
dv360_reach$Market <- str_replace(dv360_reach$Market.interim, " ", "" )

# QC if the function worked correctly
table(dv360_reach$Market)
str(dv360_reach)
table(is.na(dv360_reach$Market))

# Extract the brand
dv360_reach$Brand.interim <- str_match(dv360_reach$key, "Physiogel|Sensodyne|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Aquafresh|Poligrip|ProNamel|Contac|Zyrtec|Eno|ENO")
dv360_reach$Brand <- str_replace(dv360_reach$Brand.interim, " ", "-" )

# QC if the function worked correctly
table(dv360_reach$Brand)
str(dv360_reach)
table(is.na(dv360_reach$Brand))


## 4- add in the currency in GBP and local currency - Not needed

## 5- extract the following fields from the new taxonomy fields

# ------------------------ Extract at a Insertion Order Level -------------------- #
# market
# brand
# campaign name (cn)
# taxonomy status
# primary KPIs (pk)
# job number (jo)
# format (fm)
# objective (ob)
# creative (ff)

# ------------------------ Code to extract data at a Insertion Order level-------------------- #

dv360_reach_final <- dv360_reach

# market
dv360_reach_final$MK.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('MK~[^_]*|mk~[^_]*|Mk~[^_]*|mK~[^_]*'))
dv360_reach_final$MK.final <- str_replace(dv360_reach_final$MK.interim, "MK~|mk~|Mk~|mK", "")
dv360_reach_final$MK.final_lower <- tolower(dv360_reach_final$MK.final)

# brand
dv360_reach_final$PR.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('PR~[^_]*|pr~[^_]*|Pr~[^_]*|pR~[^_]*'))
dv360_reach_final$PR.final <- str_replace(dv360_reach_final$PR.interim, "PR~|pr~|Pr~|pR~", "")
dv360_reach_final$PR.final_lower <- tolower(dv360_reach_final$PR.final)

# campaign name
dv360_reach_final$campaign.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('CN~[^_]*|cn~[^_]*|cN~[^_]*|Cn~[^_]*'))
dv360_reach_final$campaign.final <- str_replace(dv360_reach_final$campaign.interim, "CN~|cn~|cN~|Cn~", "")

# taxonomy status
dv360_reach_final <- dplyr::mutate(dv360_reach_final, taxonomy_status = ifelse(is.na(campaign.final), "old", "new"))
table(dv360_reach_final$taxonomy_status)

# primary kpis
dv360_reach_final$PK.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('PK~[^_]*|pk~[^_]*|Pk~[^_]*|pK~[^_]*'))
dv360_reach_final$PK.final <- str_replace(dv360_reach_final$PK.interim, "PK~|pk~|pK~|Pk~", "")

# job number
dv360_reach_final$JO.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('JO~[^_]*|jo~[^_]*|jO~[^_]*|Jo~[^_]*'))
dv360_reach_final$JO.final <- str_replace(dv360_reach_final$JO.interim, "JO~|jo~|Jo~|jO~", "")

# format
dv360_reach_final$FM.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('FM~[^_]*|fm~[^_]*|fM~[^_]*|Fm~[^_]*'))
dv360_reach_final$FM.final <- str_replace(dv360_reach_final$FM.interim, "FM~|fm~|fM~|Fm~", "")
dv360_reach_final$FM.final_lower <- tolower(dv360_reach_final$FM.final)

# objective
dv360_reach_final$OB.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('OB~[^_]*|ob~[^_]*|oB~[^_]*|Ob~[^_]*'))
dv360_reach_final$OB.final <- str_replace(dv360_reach_final$OB.interim, "OB~|ob~|oB~|Ob~", "")
dv360_reach_final$OB.final_lower <- tolower(dv360_reach_final$OB.final)

# channel
dv360_reach_final$CH.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('CH~[^_]*|ch~[^_]*|Ch~[^_]*|cH~[^_]*'))
dv360_reach_final$CH.final <- str_replace(dv360_reach_final$CH.interim, "CH~|ch~|cH~|Ch~|", "")
dv360_reach_final$CH.final_lower <- tolower(dv360_reach_final$CH.final)

# creative - reach adgroup
dv360_reach_final$CR.interim <- str_match(dv360_reach_final$`Insertion Order`, regex('FF~[^_]*|ff~[^_]*|fF~[^_]*|Ff~[^_]*'))
dv360_reach_final$CR.final <- str_replace(dv360_reach_final$CR.interim, "FF~|ff~|Ff~|fF~", "")

# QC after field extraction
str(dv360_reach_final)



## 6- removing additional fields
# No need to remove currency related factors
# dv360_reach_final <- select(dv360_reach_final, -Convert_to_Pound, -Convert_to_Local, -newDate)
dv360_reach_final <- select(dv360_reach_final, -contains("interim"))
dv360_reach_final$Brand_new <- dv360_reach_final$Brand



## 7- adding the description of the taxonomy

# read the mapping table and left join with the base table

# MK
mapping_df_mk <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 8, col_names = T, col_types = 'text')
dv360_reach_final <- left_join(dv360_reach_final, mapping_df_mk, by=c("MK.final_lower" = "Code_lower"))
dv360_reach_final <- rename(dv360_reach_final, MK.Description = Description)

# PR
mapping_df_pr <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 9, col_names = T, col_types = 'text')
dv360_reach_final <- left_join(dv360_reach_final, mapping_df_pr, by=c("PR.final_lower" = "Code_lower"))
dv360_reach_final <- rename(dv360_reach_final, PR.Description = Description)

# FM
mapping_df_fm <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 1, col_names = T, col_types = 'text')
dv360_reach_final <- left_join(dv360_reach_final, mapping_df_fm, by=c("FM.final_lower" = "Code_lower"))
dv360_reach_final <- rename(dv360_reach_final, FM.Description = Description)

# OB
mapping_df_ob <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 2, col_names = T, col_types = 'text')
dv360_reach_final <- left_join(dv360_reach_final, mapping_df_ob, by=c("OB.final_lower" = "Code_lower"))
dv360_reach_final <- rename(dv360_reach_final, OB.Description = Description)

# CH
mapping_df_ch <- read_excel('taxonomy_master_dictionary.xlsx', sheet = 5, col_names = T, col_types = 'text')
dv360_reach_final <- left_join(dv360_reach_final, mapping_df_ch, by=c("CH.final_lower" = "Code_lower"))
dv360_reach_final <- rename(dv360_reach_final, CH.Description = Description)


#QC
str(dv360_reach_final)

## 8- write back the output
outputfileName <- paste0( "dv360_reach_", file_date, "_transformed.csv")
write.csv(dv360_reach_final, outputfileName, row.names=FALSE, fileEncoding = 'UTF-8')
