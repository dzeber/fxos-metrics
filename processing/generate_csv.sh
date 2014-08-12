#!/bin/bash

## Download FTU records, sanitize and output as CSV.

DATA_DIR=$HOME/fxos-ftu-data
SCRIPT_DIR=$HOME/software/fxos-metrics/processing
LOG_FILE=$DATA_DIR/processing.log
DATA_FILE=$DATA_DIR/ftu_data.out
JOB_LOG=$DATA_DIR/ftu_job.log


CSV_FILE=$DATA_DIR/data.csv
UPDATED_TIME_FILE=$DATA_DIR/last_updated


ADDR=dzeber

exec > $LOG_FILE 2>&1

# Update output files from latest run. 
echo "Downloading latest output from AWS."

rm -f $DATA_DIR/*
aws s3 cp s3://telemetry-private-analysis/fxosping/data/ $DATA_DIR --recursive

if [ ! -e "$DATA_FILE" ]; then
    # Something went wrong - no data file downloaded.
    echo "No data file!"
    # Check for log file. 
    if [ ! -e "$JOB_LOG" ]; then
        echo "No log file either!!"
        cat "-- No log file --" > $JOB_LOG
    fi
    # Send email notice with log file as text. 
    mail -s "FAILED: FxOS FTU data - no data" "$ADDR@mozilla.com" < $JOB_LOG
    echo "Sent email notice. Exiting..."
    exit
fi

# At this point we should have the latest data. 
echo "Processing data..."
python $SCRIPT_DIR/generate_csv.py $DATA_FILE $CSV_FILE
    
if [ ! -e "$CSV_FILE" ]; then
    echo "Something went wrong - no CSV file generated!"
    echo "" | mail -s "FAILED: FxOS FTU data - no csv" "$ADDR@mozilla.com" 
    exit
fi

# Look up the time the data was updated. 
echo "Recorded data update time."
date -r $DATA_FILE +"%Y-%m-%d %H:%M:%S" > $UPDATED_TIME_FILE

# Copy files to web server. 
echo "Copying data to app1."


echo "Done."
exit 0

