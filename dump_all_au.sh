#!/bin/bash

# Pass option '--nolog' to print all messages to stdout rather than log files. 
# This is mainly for testing.
LOG_TO_FILE=true
if [ $# -gt 0 ] && [ "$1" = "--nolog" ]; then
    LOG_TO_FILE=false
fi


# Dump all FxOS AU records from the start date to the present.
#START_DATE=`date +%Y%m%d -d "-9 months"`
START_DATE=20150101

CURRENT_DIR=$(pwd)
SRC_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$HOME/telemetry-server

OUTPUT_DIR="$CURRENT_DIR/output"
OUTPUT_FILE="$OUTPUT_DIR/au_raw.out"
LOG_FILE="$OUTPUT_DIR/au_raw.log"
JOB_LOG="$OUTPUT_DIR/mapred.log"
TARBALL=au_raw_dump.tar.gz

if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir "$OUTPUT_DIR"
fi

# Write output to log for debugging.
$LOG_TO_FILE && exec > $LOG_FILE 2>&1

echo "It is now `date`"
echo "Preparing job..."

WORK_DIR=$CURRENT_DIR/work
DATA_DIR=$CURRENT_DIR/data

if [ ! -d "$WORK_DIR" ]; then
    mkdir "$WORK_DIR"
fi

if [ ! -d "$DATA_DIR" ]; then
    mkdir "$DATA_DIR"
fi

JOB_FILE=$SRC_DIR/dump_all.py
FILTER=$SRC_DIR/filter.json

cp "$SRC_DIR/all_fxos_date.json" $FILTER
# Set the reason string.
sed -i'' "s/__REASON__/appusage/" $FILTER
# Set the date range.
DATE_STRING="\"min\": \""$START_DATE"\""
sed -i'' "s/__DATES__/$DATE_STRING/" $FILTER

echo "Job setup complete."
echo "Running job." 

cd "$TELEMETRY_SERVER_DIR"

# Switch logging to separate file for job output.
$LOG_TO_FILE && exec > $JOB_LOG 2>&1
python -m mapreduce.job "$JOB_FILE" \
   --input-filter "$FILTER" \
   --num-mappers 16 \
   --num-reducers 4 \
   --work-dir "$WORK_DIR" \
   --data-dir "$DATA_DIR" \
   --output "$OUTPUT_FILE" \
   --bucket "telemetry-published-v2" \
   --verbose
JOB_EXIT_CODE=$?

# Back to main log file.
$LOG_TO_FILE && exec > $LOG_FILE 2>&1
echo "Mapreduce job exited with code: $JOB_EXIT_CODE"
echo "It is now `date`"
echo "Packaging output..."

cd "$CURRENT_DIR"
tar cvzf "$TARBALL" "`basename $OUTPUT_DIR`/*"
rm -f $OUTPUT_DIR/*
mv $TARBALL $OUTPUT_DIR

echo "Done. Exiting..."

exit 0


