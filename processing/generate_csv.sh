#!/bin/bash

## Download FTU records, sanitize and output as CSV.

. ~/.bash_profile
. /etc/profile.d/mozilla.sh

SCRIPT_DIR=$(cd "`dirname "$0"`"; pwd)

. $SCRIPT_DIR/../settings.env

# Where to run the processing script from.
WORK_DIR=$HOME/fxos-ftu-data
# Subdir to contain the unpackaged job output files.
JOB_OUTPUT_DIR=$WORK_DIR/$OUTPUT_DIR_NAME
# Subdir to contain the processed data files to be copied to the web server.
DATA_DIR=$WORK_DIR/data_files


# Tarball containing job output to download from AWS.
# TARBALL=ftu_data.tar.gz
TARBALL=$JOB_TARBALL
LOG_FILE=$WORK_DIR/$PROCESSING_LOG_FILE
DATA_FILE=$JOB_OUTPUT_DIR/$JOB_OUTPUT_FILE
JOB_LOG=$JOB_OUTPUT_DIR/$JOB_LOG_FILE

CSV_PATH=$DATA_DIR/$CSV_FILE
LAST_UPDATED_PATH=$DATA_DIR/$UPDATED_TIME_FILE

ADDR=dzeber

# Flush log file once per day.
if [[ "$(date +%Y%m%d)" > "$(date -r $LOGFILE +%Y%m%d)" ]]; then
    > $LOGFILE
fi

exec >> $LOG_FILE 2>&1

# Check if the file on the server is newer than the one 
# currently showing in the dashboard.
# Check is done by date only (not by time).
SERVER_LAST_UPDATED=`aws s3 ls "$S3_FXOS/$TARBALL" | grep -Eo "^[0-9]{4}(-[0-9]{2}){2}"`
if grep -q "$SERVER_LAST_UPDATED" $LAST_UPDATED_PATH; then
    echo "Current data is up-to-date. Exiting."
    exit 2
fi


cd $WORK_DIR
rm -f $TARBALL

# Update output files from latest run. 
echo "Downloading latest output from AWS."
aws s3 cp "$S3_FXOS/$TARBALL" $WORK_DIR

## Extract tarball into WORK_DIR. 
## Creates a subdir called "output" containing files. 

if [ ! -e "$TARBALL" ]; then
    echo "Failed to download tarball from AWS!"
    echo "" | mail -s "FAILED: FxOS FTU data - no tar downloaded" \
        "$ADDR@mozilla.com" 
    exit 1
fi

rm -f $JOB_OUTPUT_DIR/*
tar xvzf $TARBALL

if [ ! -e "$DATA_FILE" ]; then
    # Something went wrong - no data file downloaded.
    echo "No data file!"
    # Check for log file. 
    if [ ! -e "$JOB_LOG" ]; then
        echo "No log file either!!"
        echo "-- No log file --" > $JOB_LOG
    fi
    # Send email notice with log file as text. 
    mail -s "FAILED: FxOS FTU data - no data" "$ADDR@mozilla.com" < $JOB_LOG
    echo "Sent email notice. Exiting..."
    exit 1
fi

# At this point we should have the latest data. 
echo "Processing data..."
python $SCRIPT_DIR/generate_csv.py $DATA_FILE $CSV_PATH
    
if [ ! -e "$CSV_PATH" ]; then
    echo "Something went wrong - no CSV file generated!"
    echo "" | mail -s "FAILED: FxOS FTU data - no csv" "$ADDR@mozilla.com" 
    exit 1
fi

# Look up the time the data was updated. 
echo "Recorded data update time."
date -r $DATA_FILE +"%Y-%m-%d %H:%M:%S" > $DATA_DIR/$UPDATED_TIME_FILE

# Copy files to web server. 
echo "Copying data to app1."
# cd $WORK_DIR

# Append underscore to existing data files. 
ssh $APP1 ". .bash_profile;
    cd \$FTU/data; \
    rm -f *_ ; \
    mv $CSV_FILE ${CSV_FILE}_; \
    mv $UPDATED_TIME_FILE ${UPDATED_TIME_FILE}_"
    
# tar czf new_data.tar.gz $CSV_FILE $UPDATED_TIME_FILE 
cd $DATA_DIR
tar czf $WORK_DIR/new_data.tar.gz ./*
cd $WORK_DIR
scp new_data.tar.gz "$APP1:\$HOME"
ssh $APP1 ". .bash_profile; \
    tar xzf new_data.tar.gz -C \$FTU/data; \
    chmod o+r \$FTU/data/*; \
    rm new_data.tar.gz"
rm new_data.tar.gz

echo "Done."
exit 0

