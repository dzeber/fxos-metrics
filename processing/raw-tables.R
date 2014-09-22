#######################################################################
###  
###  Process raw FTU data. 
###  Download from server, flatten JSON and convert to a data.table.
###  Apply some preliminary cleansing.
###  
#######################################################################


library(rjson)


## Download gzipped raw data dump from AWS server, 
## and convert it to a data table. 

get.ftu <- function(server, datafile, destpath) {
    datafile <- download.ftu(server, datafile, destpath)
    dd <- convert.ftu(datafile)
    sanitize.ftu(dd, sub("\\.out(\\.gz)?$", "", datafile))
}


## Sanitize the FTU data table by formatting strings, deduplicating,
## and removing redundant fields. 
## If savepath is not null, result will be saved. 
sanitize.ftu <- function(dd, savepath = NULL) {
    cat("Sanitizing table...\n")

    ## Remove display properties. 
    dd[, devicePixelRatio := NULL]
    dd[, screenHeight := NULL][, screenWidth := NULL]
    
    ## Remove platform info.
    # dd[, platform_version := NULL]
    dd[, platform_build_id := NULL]
    
    ## Date.
    
    ## Add ping date. 
    dd[, pingDate := as.Date(pingTime)]
    ## Remove skewed dates.
    dd <- dd[pingDate > "2014-04-01" & pingDate <= Sys.Date()]
    ## Remove activation times and ping times.
    dd[, pingTime := NULL][, activationTime := NULL]

    ## Update channel. 
    
    ## Should have at most one non-missing of app.update.channel 
    ## and update_channel. 
    ## Merge them.
    no.channel <- dd[, is.na(update_channel)]
    dd[no.channel, update_channel := app.update.channel]
    ## Remove app.update.channel, unless there are any inconsistent values. 
    if(!any(dd[!no.channel, !is.na(app.update.channel) & 
            app.update.channel != update_channel]))
        dd[, app.update.channel := NULL]
        
    ## Country.
    
    ## Add country name. 
    if(!exists("country.name"))
        load("~/github/work-tools/R/other/country-codes/countrycodes.RData")
    dd[, country := country.name(geoCountry, NA)]
    
    ## Mobile codes.
    
    ## Remove network carrier and region fields (redundant).
    dd[, network.region := NULL][, network.carrier := NULL]
    
    
    ############################
    
    ## Save and output table if required.
    if(!is.null(savepath)) {
        if(!grepl("\\.RData$", savepath))
            savepath <- sprintf("%s.RData", savepath)
            
        cat(sprintf("Saving tables to %s.\n", savepath))
        ftu <- dd
        save(ftu, file = savepath)
    }
    
    cat("Done.\n")
    ftu
}


## Download the data from the EC2 instance.
## Return the path to the downloaded data file.
##
## Defaults to preset connection named "aws", 
## gzipped file dated with today's date,
## and getwd()/data. 
download.ftu <- function(server, datafile, destpath) {
    # Default server name is preset connection. 
    server <- if(missing(server)) "aws" else sprintf("ubuntu@%s", server)
    # Default file name is today's date in the file name. 
    if(missing(datafile)) datafile <- sprintf("ftu_%s.out.gz", Sys.Date())
    # Default destination is ./data/.
    if(missing(destpath)) destpath <- file.path(getwd(), "data")
    
    ## Download data.
    cat("Downloading data...\n")
    shell(sprintf("pscp %s:/home/ubuntu/%s \"%s\"", server, datafile, destpath))
    ## Return data file path.
    file.path(destpath, datafile)
}


## Convert raw JSON payloads to a data table.
## This is still "raw", in the sense that the fields are not modified. 
## Save the resulting table to same dir as datafile, with the suffix "raw".
## Also saves list of bad records for later inspection, if any.
## Returns the table.
convert.ftu <- function(datafile) {
    require(rjson)
    
    cat("Loading data...\n")
    dd <- readLines(datafile)

    ## Drop UUIDs and convert to R lists. 
    dd <- unlist(lapply(strsplit(dd, "\t"), "[[", 2))
    dd <- lapply(dd, fromJSON)
    
    ## Isolate weird records to check afterwards. 
    cat("Checking for data consistency...\n")
    ftu.bad <- list()
    
    ## Check consistency of info fields. 
    consistent.info <- function(r) {
        all(identical(r$info$appName, "FirefoxOS"),
            identical(r$info$reason, "ftu"), 
            is.null(r$deviceinfo.update_channel) || identical(r$info$appUpdateChannel, r$deviceinfo.update_channel),
            identical(r$info$appVersion, r$deviceinfo.platform_version),
            identical(r$info$appBuildID, r$deviceinfo.platform_build_id))
    }
    cons <- unlist(lapply(dd, consistent.info))
    if(!all(cons)) {
        cat(sprintf(paste("  >> Some inconsistency between info and deviceinfo:",
            "%s bad records.\n",
            "Removing inconsistent records.\n"), sum(!cons)))
        ftu.bad[["inconsistent"]] <- dd[!cons]
        dd <- dd[cons]
    }
    
    ## Keep only geoCountry from info (other fields are redundant), 
    ## and remove payload version.
    ## Remove deviceinfo prefix form field names.
    ## Flatten structure and remove NULLs. 
    cat("Restructuring...\n")
    dd <- lapply(dd, function(r) { 
        r$geoCountry <- r$info$geoCountry
        r$info <- NULL
        r$ver <- NULL
        
        ## Remove deviceinfo prefix from names. 
        names(r) <- sub("^deviceinfo\\.", "", names(r))
        ## Flatten structure - should be at most 1 level of nesting.
        if(any(sapply(r, length) > 1)) 
            r <- unlist(r, recursive = FALSE)
        ## Remove NULL or length 0 fields. 
        r[sapply(r, length) == 0] <- NULL
        
        r
    })

    ## Check that records are flat, and that there are no NULLs.
    scalar = sapply(dd, function(r) { any(sapply(r, length) == 1) })
    if(!all(scalar)) {
        cat(paste("  >> Some records were not flattened",
            "or still contain NULL fields.",
            "Removing malformed records.\n"))
        ftu.bad[["malformed"]] <- dd[!scalar]
        dd <- dd[scalar]
    }
    
    ############################
    
    ## Convert to data table, and convert missing/empty entries to NA. 
    
    cat("Converting to data table...\n")
    
    ## Collect set of unique names. 
    nm <- unique(unlist(lapply(dd, names)))

    dd <- lapply(dd, function(r) {
        setNames(lapply(nm, function(nn) { 
            v <- r[nn][[1]] 
            if(is.null(v) || length(v) == 0 || !nzchar(v)) NA else v
        }), nm)
    })
    
    ## Set types of first row explicitly to keep data.table happy. 
    ## Certain fields will be numeric. Otherwise, default is character.
    to.numeric <- c("devicePixelRatio", "screenHeight", "pingTime",
        "screenWidth", "activationTime")
    num <- nm %in% to.numeric
    dd[[1]][num] <- lapply(dd[[1]][num], as.numeric)
    dd[[1]][!num] <- lapply(dd[[1]][!num], as.character)
    dd <- rbindlist(dd)

    ## Convert timestamps to datetime.
    ts.to.datetime <- function(a) { 
        as.POSIXct(a / 1000, origin = "1970-01-01")
    }
    dd[, activationTime := ts.to.datetime(activationTime)]
    dd[, pingTime := ts.to.datetime(pingTime)]
    
    ############################
    
    ## Save and output tables.
    
    datafile <- sub("\\.out(\\.gz)?$", "_raw.RData", datafile)
    cat(sprintf("Saving tables to %s.\n", datafile))
    
    ftu.raw <- dd
    tbls <- "ftu.raw"
    if(length(ftu.bad) > 0) tbls <- c(tbls, "ftu.bad")
    
    ## Save processed data to file. 
    save(list = tbls, file = datafile)
    
    cat("Done.\n")
    dd
}


