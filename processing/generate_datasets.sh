#!/bin/bash

## Download FTU records, sanitize and generate CSV datasets.
## Copy resulting datasets to web server. 

# Environment for communicating with AWS and app1.
. ~/.bash_profile
. /etc/profile.d/mozilla.sh

. settings.env

# The base dir for the processing script. 
# Also the working dir for the dashboard data.
WORK_DIR=$HOME/fxos-data/ftu
# Where to unpack and process the dump data.
DUMP_WORK_DIR=$WORK_DIR/aws_job
# Subdir to contain the processed data files to be copied to the web server.
DATA_DIR=$WORK_DIR/data_files

TARBALL=$DUMP_WORK_DIR/$DUMP_TARBALL
JOB_OUTPUT=$DUMP_WORK_DIR/$OUTPUT_DIR_NAME
OUTPUT_DATA=$JOB_OUTPUT/$DUMP_FILE
OUTPUT_LOG=$JOB_OUTPUT/$JOB_LOG_FILE

PYTHON_SCRIPT=generate_datasets.py
LOG_FILE=$WORK_DIR/$PROCESSING_LOG_FILE
LAST_UPDATED_PATH=$DATA_DIR/$UPDATED_TIME_FILE
DASHBOARD_CSV_PATH=$DATA_DIR/$CSV_FILE
DUMP_CSV_PATH=$DATA_DIR/$DUMP_CSV

ADDR=dzeber

exec >> $LOG_FILE 2>&1

# Flush log file once per day.
if [ -e $LOG_FILE ] && [[ "$(date +%Y%m%d)" > "$(date -r $LOG_FILE +%Y%m%d)" ]]; then
    > $LOG_FILE
fi


echo "------------"
echo
echo "Starting processing script: `date`."

# Check whether new data is available. 
SERVER_LAST_UPDATED=`aws s3 ls "$S3_FXOS_DUMP/$DUMP_TARBALL" | \
    grep -Eo "^[0-9]{4}(-[0-9]{2}){2}"`

# If not, nothing to do.
if grep -q "$SERVER_LAST_UPDATED" $LAST_UPDATED_PATH; then
    echo "Current data is up-to-date."
    echo "Done: `date`."
    echo
    exit 0
fi

# Download new data, if available, process, and copy to server.
# cd $DUMP_WORK_DIR
rm -f $TARBALL
echo "Downloading latest output from AWS."
aws s3 cp "$S3_FXOS_DUMP/$DUMP_TARBALL" "$DUMP_WORK_DIR"

if [ ! -e "$TARBALL" ]; then
    echo "Failed to download tarball from AWS."
    echo "" | mail -s "FAILED: FxOS FTU data - unable to download $DUMP_TARBALL" \
        "$ADDR@mozilla.com" 
    echo "Sent email notice. Exiting..."
    exit 1
fi

# Extract tarball - creates a subdir called "output" containing files. 
rm -f $JOB_OUTPUT/*
tar xvzf $TARBALL -C  $DUMP_WORK_DIR

if [ ! -s "$OUTPUT_DATA" ]; then
    # Something went wrong - no data file downloaded.
    echo "No data file."
    # Check for log file. 
    if [ ! -e "$OUTPUT_LOG" ]; then
        echo "No log file either."
        echo "-- No log file --" > $OUTPUT_LOG
    fi
    # Send email notice with log file as text. 
    mail -s "FAILED: FxOS FTU data - no data file $DUMP_FILE" "$ADDR@mozilla.com" < $OUTPUT_LOG
    echo "Sent email notice. Exiting..."
    exit 1
fi

# At this point we should have the latest data. 
echo "Processing data..."
python $PYTHON_SCRIPT $OUTPUT_DATA $DASHBOARD_CSV_PATH $DUMP_CSV_PATH
    
if [ ! -e "$DASHBOARD_CSV_PATH" ]; then
    echo "Something went wrong - no dashboard CSV file generated."
    echo "" | mail -s "FAILED: FxOS FTU data - no csv `$CSV_FILE`" "$ADDR@mozilla.com" 
    exit 1
fi
if [ ! -e "$DUMP_CSV_PATH" ]; then
    echo "Something went wrong - no dump CSV file generated."
    echo "" | mail -s "FAILED: FxOS FTU data - no csv `$DUMP_CSV`" "$ADDR@mozilla.com" 
    exit 1
fi

# Update the last updated time.
echo "Done. Recording data update time."
date -r $OUTPUT_DATA +"%Y-%m-%d %H:%M:%S" > $LAST_UPDATED_PATH
        
# Copy new data to web server.
# Append underscore to existing data files names on server. 
UNDERSCORE_CMD="ssh \$APP1 \". .bash_profile; cd \\\$FTU/data; rm -f *_;"
UNDERSCORE_CMD="$UNDERSCORE_CMD mv $CSV_FILE ${CSV_FILE}_;" 
UNDERSCORE_CMD="$UNDERSCORE_CMD mv $DUMP_CSV ${DUMP_CSV}_"
UNDERSCORE_CMD="$UNDERSCORE_CMD\""
eval $UNDERSCORE_CMD

cd $DATA_DIR
tar czf $WORK_DIR/new_data.tar.gz $CSV_FILE $DUMP_CSV $UPDATED_TIME_FILE
cd $WORK_DIR
scp new_data.tar.gz "$APP1:\$HOME"
ssh $APP1 ". .bash_profile; \
    tar xzf new_data.tar.gz -C \$FTU/data; \
    chmod o+r \$FTU/data/*; \
    rm new_data.tar.gz"
rm new_data.tar.gz

echo "Done: `date`."
exit 0

