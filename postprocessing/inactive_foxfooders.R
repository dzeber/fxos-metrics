#######################################################################
###  
###  Identify inactive Foxfooders from submitted AU data.
###  
#######################################################################


library(data.table)

tryCatch({

## Load table of dogfooder info.
dfd <- as.data.table(read.csv("../data_files/dogfood_details.csv", 
    stringsAsFactors = FALSE))
## Interpret deviceID numbers as strings.
dfd[, deviceID := as.character(deviceID)]
## Restrict to those that have had app usage since Whistler.
dfd <- dfd[latest_appusage > "2015-06-22"]

## Load list of dogfooder IMEIs.
imeis <- readLines("foxfood_imei.txt")
## Restrict table to those devices in the list.
dfd <- dfd[deviceID %in% imeis]

## To be contacted: those whose latest app usage was more than 21 days ago.
dfd[, to_be_contacted := as.Date(latest_appusage) < Sys.Date() - 21]

## Table of those to be contacted. 
tbc <- dfd[to_be_contacted == TRUE, 
    list(deviceID, earliest_ping = earliest_submission, 
        latest_ping = latest_submission, earliest_appusage,
        latest_appusage, num_pings_received = num_pings, country)]
write.csv(tbc, file = "inactive_foxfooders.csv", row.names = FALSE, na = "")

## List of foxfood devices not yet activated.
imeihasinfo <- dfd[, imeis %in% deviceID]
write(imeis[!imeihasinfo], file = "unactivated_foxfooders.txt", ncolumns = 1)

}, error = function(e) {
    ## If anything goes wrong, send email with error message.
    system(sprintf(
        "echo '%s' | mailx -s 'Foxfood IMEI job failed!' dzeber@mozilla.com",
        e$message))
})
