#!/bin/bash

## Download FTU records, sanitize and output as CSV.

DATA_DIR=$HOME/fxos-ftu-data
SCRIPT_DIR=$(cd "`dirname "$0"`"; pwd)
LOG_FILE=$DATA_DIR/processing.log
DATA_FILE=$DATA_DIR/ftu_data.out
JOB_LOG=$DATA_DIR/ftu_job.log

CSV_FILENAME=data.csv
CSV_PATH=$DATA_DIR/$CSV_FILENAME
UPDATED_TIME_FILENAME=last_updated
UPDATED_TIME_PATH=$DATA_DIR/$UPDATED_TIME_FILENAME

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
python $SCRIPT_DIR/generate_csv.py $DATA_FILE $CSV_PATH
    
if [ ! -e "$CSV_PATH" ]; then
    echo "Something went wrong - no CSV file generated!"
    echo "" | mail -s "FAILED: FxOS FTU data - no csv" "$ADDR@mozilla.com" 
    exit
fi

# Look up the time the data was updated. 
echo "Recorded data update time."
date -r $DATA_FILE +"%Y-%m-%d %H:%M:%S" > $UPDATED_TIME_PATH

# Copy files to web server. 
echo "Copying data to app1."
cd $DATA_DIR
# Append underscore to existing data files. 
ssh $APP1 ". .bash_profile;
    cd \$FTU/data; \
    rm -f *_ ; \
    mv $CSV_FILENAME ${CSV_FILENAME}_; \
    mv $UPDATED_TIME_FILENAME ${UPDATED_TIME_FILENAME}_"
    
tar czf new_data $CSV_FILENAME $UPDATED_TIME_FILENAME 
scp new_data "$APP1:\$HOME"
ssh $APP1 ". .bash_profile; \
    tar xzf new_data -C \$FTU/data; \
    chmod o+r \$FTU/data/$CSV_FILENAME \$FTU/data/$UPDATED_TIME_FILENAME; \
    rm new_data"
rm new_data

echo "Done."
exit 0

