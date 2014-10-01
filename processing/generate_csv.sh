#!/bin/bash

## Download FTU records, sanitize and output as CSV.

. ~/.bash_profile
. /etc/profile.d/mozilla.sh

SCRIPT_DIR=$(cd "`dirname "$0"`"; pwd)
PYTHONPATH="$SCRIPT_DIR/../shared"

. $SCRIPT_DIR/../settings.env

# The base dir for the processing script. 
# Also the working dir for the dashboard data.
WORK_DIR=$HOME/fxos-ftu-data
# Where to unpack and process the dump data.
DUMP_WORK_DIR=$WORK_DIR/ftu-dump
# Subdir to contain the processed data files to be copied to the web server.
DATA_DIR=$WORK_DIR/data_files

DASHBOARD_DATA_SCRIPT=generate_csv.py
DUMP_DATA_SCRIPT=generate_dump_csv.py

LOG_FILE=$WORK_DIR/$PROCESSING_LOG_FILE
LAST_UPDATED_PATH=$DATA_DIR/$UPDATED_TIME_FILE

ADDR=dzeber

exec >> $LOG_FILE 2>&1

# Flush log file once per day.
if [ -e $LOG_FILE ] && [[ "$(date +%Y%m%d)" > "$(date -r $LOG_FILE +%Y%m%d)" ]]; then
    > $LOG_FILE
fi


# Check if the file on the server is newer than the one 
# currently showing in the dashboard.
# Check is done by date only (not by time).
# Use option --dump to specify working with dump output.
up_to_date(){
    if [[ "$1" == "--dump" ]]; then
        AWS_PATH="$S3_FXOS_DUMP/$DUMP_TARBALL"
    else
        AWS_PATH="$S3_FXOS/$JOB_TARBALL"
    fi
    
    SERVER_LAST_UPDATED=`aws s3 ls "$AWS_PATH" | grep -Eo "^[0-9]{4}(-[0-9]{2}){2}"`
    grep -q "$SERVER_LAST_UPDATED" $LAST_UPDATED_PATH
}


# Download new data, if available, process, and copy to server.
# Use option --dump to specify working with dump output.
# Args: working dir.
update_data(){
    ## Initialize settings.
    if [[ "$1" == "--dump" ]]; then
        shift
        CURRENT_WD="$1"
        CURRENT_TAR=$DUMP_TARBALL
        AWS_PATH=$S3_FXOS_DUMP/$DUMP_TARBALL
        # Subdir to contain the unpackaged job output files.
        JOB_OUTPUT_DIR=$CURRENT_WD/$OUTPUT_DIR_NAME
        DATA_FILE=$JOB_OUTPUT_DIR/$DUMP_FILE
        JOB_LOG=$JOB_OUTPUT_DIR/$JOB_LOG_FILE
        SCRIPT_NAME=$DUMP_DATA_SCRIPT
        CSV_PATH=$DATA_DIR/$DUMP_CSV
    else
        CURRENT_WD="$1"
        CURRENT_TAR=$JOB_TARBALL
        AWS_PATH=$S3_FXOS/$JOB_TARBALL
        # Subdir to contain the unpackaged job output files.
        JOB_OUTPUT_DIR=$CURRENT_WD/$OUTPUT_DIR_NAME
        DATA_FILE=$JOB_OUTPUT_DIR/$JOB_OUTPUT_FILE
        JOB_LOG=$JOB_OUTPUT_DIR/$JOB_LOG_FILE
        SCRIPT_NAME=$DASHBOARD_DATA_SCRIPT
        CSV_PATH=$DATA_DIR/$CSV_FILE
    fi
    
    cd $CURRENT_WD
    rm -f $CURRENT_TAR

    # Update output files from latest run. 
    echo "Downloading latest output from AWS."
    aws s3 cp "$AWS_PATH" "$CURRENT_WD"

    ## Extract tarball into CURRENT_WD. 
    ## Creates a subdir called "output" containing files. 

    if [ ! -e "$CURRENT_TAR" ]; then
        # echo "Failed to download tarball from AWS!"
        echo "" | mail -s "FAILED: FxOS FTU data - unable to download $CURRENT_TAR" \
            "$ADDR@mozilla.com" 
        return 1
    fi

    rm -f $JOB_OUTPUT_DIR/*
    tar xvzf $CURRENT_TAR

    if [ ! -s "$DATA_FILE" ]; then
        # Something went wrong - no data file downloaded.
        # echo "No data file!"
        # Check for log file. 
        if [ ! -e "$JOB_LOG" ]; then
            # echo "No log file either!!"
            echo "-- No log file --" > $JOB_LOG
        fi
        # Send email notice with log file as text. 
        mail -s "FAILED: FxOS FTU data - no data file `basename $DATA_FILE`" "$ADDR@mozilla.com" < $JOB_LOG
        echo "Sent email notice. Exiting..."
        return 1
    fi

    # At this point we should have the latest data. 
    echo "Processing data..."
    python $SCRIPT_DIR/$SCRIPT_NAME $DATA_FILE $CSV_PATH
        
    if [ ! -e "$CSV_PATH" ]; then
        # echo "Something went wrong - no CSV file generated!"
        echo "" | mail -s "FAILED: FxOS FTU data - no csv `basename $CSV_PATH`" "$ADDR@mozilla.com" 
        return 1
    fi
    
    echo "Done."
    return 0
}

FILES_TO_UPDATE=""

echo
echo "------------"
echo
echo "Starting processing script: `date`."

echo "Processing dashboard data."

if up_to_date; then
    echo "Current dashboard data is up-to-date."
else
    if update_data "$WORK_DIR"; then
        # If update succeeded, look up the time the data was updated. 
        echo "Recording data update time."
        date -r $WORK_DIR/$OUTPUT_DIR_NAME/$JOB_OUTPUT_FILE \
            +"%Y-%m-%d %H:%M:%S" > $DATA_DIR/$UPDATED_TIME_FILE
        FILES_TO_UPDATE="$CSV_FILE $UPDATED_TIME_FILE"
    fi
fi

echo "Processing dump data."
if up_to_date --dump; then
    echo "Current data dump is up-to-date."
else
    if update_data --dump "$DUMP_WORK_DIR"; then
        FILES_TO_UPDATE="$FILES_TO_UPDATE $DUMP_CSV"
    fi
fi

## Copy any new data files to app1.
if [[ "$FILES_TO_UPDATE" ]]; then
    # Copy files to web server. 
    echo "Copying data to app1."

    # Append underscore to existing data files. 
    UNDERSCORE_CMD="ssh \$APP1 \". .bash_profile; cd \\\$FTU/data; rm -f *_"
    for f in $FILES_TO_UPDATE; do
        UNDERSCORE_CMD="$UNDERSCORE_CMD; mv $f ${f}_"
    done
    UNDERSCORE_CMD="$UNDERSCORE_CMD\""
    eval $UNDERSCORE_CMD
    
    cd $DATA_DIR
    eval "tar czf $WORK_DIR/new_data.tar.gz $FILES_TO_UPDATE" 
    cd $WORK_DIR
    scp new_data.tar.gz "$APP1:\$HOME"
    ssh $APP1 ". .bash_profile; \
        tar xzf new_data.tar.gz -C \$FTU/data; \
        chmod o+r \$FTU/data/*; \
        rm new_data.tar.gz"
    rm new_data.tar.gz
fi

echo "Done: `date`."
exit 0


