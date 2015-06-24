#!/bin/bash

## Download AU records, sanitize and generate CSV datasets.
## Copy resulting datasets to web server. 

## Pass option --nocopy to prevent from copying files to web server.
## For testing purposes. 

# Environment for communicating with AWS and app1.
. ~/.bash_profile
. /etc/profile.d/mozilla.sh

SRC_DIR=$(cd "`dirname "$0"`"; pwd)

# . $SRC_DIR/settings.env

# The base dir for the processing script. 
# Also the working dir for the dashboard data.
WORK_DIR=$HOME/fxos-data/au
# Where to unpack and process the dump data.
DUMP_WORK_DIR=$WORK_DIR/aws_job
# Subdir to contain the processed data files to be copied to the web server.
DATA_DIR=$WORK_DIR/data_files
DUMP_TARBALL=au_dump.tar.gz

TARBALL=$DUMP_WORK_DIR/$DUMP_TARBALL
# JOB_OUTPUT=$DUMP_WORK_DIR
# OUTPUT_DATA=$JOB_OUTPUT/au_data.out
DUMP_FILE=au_data.out
OUTPUT_DATA=$DUMP_WORK_DIR/$DUMP_FILE
# OUTPUT_LOG=$JOB_OUTPUT/au_job.log
OUTPUT_LOG=$DUMP_WORK_DIR/au_job.log

PYTHON_MODULE=postprocessing.au_data_tables
LOG_FILE=$WORK_DIR/processing.log
LAST_UPDATED_PATH=$DATA_DIR/last_updated
# DASHBOARD_CSV_PATH=$DATA_DIR/$CSV_FILE
# DUMP_CSV_PATH=$DATA_DIR/$DUMP_CSV

ADDR=$USER

exec >> $LOG_FILE 2>&1

# Flush log file once per day.
if [ -e $LOG_FILE ] && [[ "$(date +%Y%m%d)" > "$(date -r $LOG_FILE +%Y%m%d)" ]]; then
    > $LOG_FILE
fi


echo "------------"
echo
echo "Starting processing script: `date`."

# Check whether new data is available. 
SERVER_LAST_UPDATED=`aws s3 ls "$S3_FXOS_AU/$DUMP_TARBALL" | \
    grep -Eo "^[0-9]{4}(-[0-9]{2}){2}"`

# If not, nothing to do.
if [ -e "$LAST_UPDATED_PATH" ] && grep -q "$SERVER_LAST_UPDATED" $LAST_UPDATED_PATH; then
    echo "Current data is up-to-date."
    echo "Done: `date`."
    echo
    exit 0
fi

# Download new data, if available, process, and copy to server.
rm -f $TARBALL
echo "Downloading latest output from AWS."
aws s3 cp "$S3_FXOS_AU/$DUMP_TARBALL" "$DUMP_WORK_DIR"

if [ ! -e "$TARBALL" ]; then
    echo "Failed to download tarball from AWS."
    echo "" | mail -s "FAILED: FxOS AU data - unable to download $DUMP_TARBALL" \
        "$ADDR@mozilla.com" 
    echo "Sent email notice. Exiting..."
    exit 1
fi

# Extract tarball - creates a subdir called "output" containing files. 
# rm -f $JOB_OUTPUT/*
# tar xvzf $TARBALL -C  $DUMP_WORK_DIR
tar xvzf $TARBALL

if [ ! -s "$OUTPUT_DATA" ]; then
    # Something went wrong - no data file downloaded.
    echo "No data file."
    # Check for log file. 
    if [ ! -e "$OUTPUT_LOG" ]; then
        echo "No log file either."
        echo "-- No log file --" > $OUTPUT_LOG
    fi
    # Send email notice with log file as text. 
    mail -s "FAILED: FxOS AU data - no data file $DUMP_FILE" "$ADDR@mozilla.com" < $OUTPUT_LOG
    echo "Sent email notice. Exiting..."
    exit 1
fi

# At this point we should have the latest data. 
echo "Processing data..."
cd $SRC_DIR
python -m $PYTHON_MODULE $OUTPUT_DATA $DATA_DIR

if [ ! "ls -1 $DATA_DIR | grep -q '\.csv$'" ]; then
    echo "Something went wrong - no CSV files generated."
    echo "" | mail -s "FAILED: FxOS AU data - no csv files" "$ADDR@mozilla.com" 
    exit 1
fi

# if [ ! -e "$DASHBOARD_CSV_PATH" ]; then
    # echo "Something went wrong - no dashboard CSV file generated."
    # echo "" | mail -s "FAILED: FxOS FTU data - no csv `$CSV_FILE`" "$ADDR@mozilla.com" 
    # exit 1
# fi
# if [ ! -e "$DUMP_CSV_PATH" ]; then
    # echo "Something went wrong - no dump CSV file generated."
    # echo "" | mail -s "FAILED: FxOS FTU data - no csv `$DUMP_CSV`" "$ADDR@mozilla.com" 
    # exit 1
# fi

# Update the last updated time.
echo "Done. Recording data update time."
date -r $OUTPUT_DATA +"%Y-%m-%d %H:%M:%S" > $LAST_UPDATED_PATH
        
        
if [[ "$1" == "--nocopy" ]]; then
    echo "Data files will not be copied to web server."
    echo "Done: `date`."
    exit 0
fi

# Copy new data to web server.
echo "Copying data files to web server."
tar cvfz csvs.tar.gz *.csv
scp csvs.tar.gz "$WWW:$(ssh $WWW ". .bash_profile; echo \$AU_CSV")"
REMOTE_CMD=". .bash_profile; cd \$AU_CSV; tar xvfz csvs.tar.gz; rm csvs.tar.gz"
REMOTE_CMD="$REMOTE_CMD; chmod 644 *"
ssh $WWW "$REMOTE_CMD"

# echo "Pushing to s3."
# aws --profile metricsprogram s3 cp $DASHBOARD_CSV_PATH "$S3_DASHBOARD/$CSV_FILE"
# # aws --profile metricsprogram s3 cp $DUMP_CSV_PATH "$S3_DASHBOARD/$DUMP_CSV"
# aws --profile metricsprogram s3 cp $LAST_UPDATED_PATH "$S3_DASHBOARD/$UPDATED_TIME_FILE"

echo "Done: `date`."
exit 0

