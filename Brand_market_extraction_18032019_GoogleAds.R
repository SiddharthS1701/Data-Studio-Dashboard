# set the working directory
workingLocation <- 'C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/working directory/search'
setwd(workingLocation)
getwd()

# install the package
# install.packages('stringr')
# install.packages('bigQueryR')

# load the libaries
library(stringr)
library(data.table)
library(dplyr)
#library(bigQueryR)

# read the currency data
currency <- fread('C:/Users/sidshriv2/Desktop/Projects/01.Delivery/10.GSK/16.Data engineering dashboard/01.Input data/currency/currency_data.csv')

#---------------------------------------------Google Ads - Main file---------------------------------------------

## read the input file

search_weekly_input <- 'search_weekly_20190901.csv'
search_weekly_output <- 'search_weekly_20190901_transformed.csv'

search_weekly <- fread(search_weekly_input)
str(search_weekly)

## fixing the date 
search_weekly$Date <- as.Date(search_weekly$Day, format = "%m/%d/%Y")
str(search_weekly)

## create the key
## Extract the market & the brand
search_weekly$key <- paste(search_weekly$Campaign, "-", search_weekly$Account)
search_weekly$Market <- str_match(search_weekly$key, "AU|NZ|HK|TW|VN|SG|MY|PH|TH|ID|KR|Korea|Indonesia|Malaysia|Thailand|Australia")
search_weekly$Brand <- str_match(search_weekly$key, "Physiogel|Sensodyne|'Panadol OA'|'Panadol C&F'|'Panadol Topical (Emulgel)'|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Zyrtec|Eno|ENO")

## QC the new columns
table(search_weekly$Market)
table(is.na(table(search_weekly$Market)))

table(search_weekly$Brand)
table(is.na(table(search_weekly$Brand)))

## Add a few extra identifier columns
search_weekly$client <- "GSK"
search_weekly$reportingChannel <- "Adwords"
str(search_weekly)

## write back the output
write.csv(search_weekly, search_weekly_output, row.names=FALSE, fileEncoding = "UTF-8")



#---------------------------------------------Google Ads - Ads file---------------------------------------------

## read the input file

search_weekly_ads_input <- 'search_weekly_ads_20190901.csv'
search_weekly_ads_output <- 'search_weekly_ads_20190901_transformed.csv'

search_weekly_ads <- fread(search_weekly_ads_input)
str(search_weekly_ads)

## fixing the date 
search_weekly_ads$Date <- as.Date(search_weekly_ads$Day, format = "%m/%d/%Y")
str(search_weekly_ads)

## create the key
## Extract the market & the brand
search_weekly_ads$key <- paste(search_weekly_ads$Campaign, "-", search_weekly_ads$Account)
search_weekly_ads$Market <- str_match(search_weekly_ads$key, "JP|AU|NZ|HK|TW|VN|SG|MY|PH|TH|ID|KR|Korea|Indonesia|Malaysia|Thailand|Australia")
search_weekly_ads$Brand <- str_match(search_weekly_ads$key, "Physiogel|Sensodyne|'Panadol OA'|'Panadol C&F'|'Panadol Topical (Emulgel)'|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Zyrtec|Eno|ENO")

## QC the new columns

table(search_weekly_ads$Market)
table(is.na(table(search_weekly_ads$Market)))

table(search_weekly_ads$Brand)
table(is.na(table(search_weekly_ads$Brand)))

## Add a few extra identifier columns
search_weekly_ads$client <- "GSK"
search_weekly_ads$reportingChannel <- "Adwords"
str(search_weekly_ads)

## write back the output
write.csv(search_weekly_ads, search_weekly_ads_output, row.names=FALSE)


#---------------------------------------------Google Ads - Audience file---------------------------------------------

## read the input file

search_weekly_audience_input <- 'search_weekly_audience_20190901.csv'
search_weekly_audience_output <- 'search_weekly_audience_20190901_transformed.csv'

search_weekly_audience <- fread(search_weekly_audience_input)
str(search_weekly_audience)

## fixing the date 
search_weekly_audience$Date <- as.Date(search_weekly_audience$Day, format = "%m/%d/%Y")
str(search_weekly_audience)

## create the key
## Extract the market & the brand
search_weekly_audience$key <- paste(search_weekly_audience$Campaign, "-", search_weekly_audience$Account)
search_weekly_audience$Market <- str_match(search_weekly_audience$key, "JP|AU|NZ|HK|TW|VN|SG|MY|PH|TH|ID|KR|Korea|Indonesia|Malaysia|Thailand|Australia")
search_weekly_audience$Brand <- str_match(search_weekly_audience$key, "Physiogel|Sensodyne|'Panadol OA'|'Panadol C&F'|'Panadol Topical (Emulgel)'|Otrivin|Acne|Scott|Horlicks|Voltaren|Blockwash|Panadol|Polident|Parodontax|Calpol|Oilatum|Sinecod|Nicabate|Duodart|Zovirax|Macleans|Biotene|Sensoydne|Flixonase|Nicotinell|Avodart|CalVive|Lamisil|Zyrtec|Eno|ENO")

## QC the new columns
table(search_weekly_audience$Market)
table(search_weekly_audience$Brand)

## Add a few extra identifier columns
search_weekly_audience$client <- "GSK"
search_weekly_audience$reportingChannel <- "Adwords"
str(search_weekly_audience)

## write back the output
write.csv(search_weekly_audience, search_weekly_audience_output, row.names=FALSE)

