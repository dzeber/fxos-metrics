#!/bin/bash

# Dump all FxOS records dated within the last 3 months of today.

BASE=$(pwd)
THIS_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$HOME/telemetry-server

. settings.env

OUTPUT_DIR=$BASE/$OUTPUT_DIR_NAME
OUTPUT_FILE=$OUTPUT_DIR/$DUMP_FILE
LOG_FILE=$OUTPUT_DIR/$JOB_LOG_FILE
BOTO_LOG=$OUTPUT_DIR/$BOTO_LOG_FILE
JOB_LOG=$OUTPUT_DIR/$MAPRED_LOG_FILE
TARBALL=$DUMP_TARBALL

if [ ! -d "$OUTPUT_DIR" ]; then
    mkdir "$OUTPUT_DIR"
fi

# Write output to log for debugging. 
exec > $LOG_FILE 2>&1

echo "It is now `date`"
echo "Preparing job..."

WORK_DIR=$BASE/work
DATA_DIR=$BASE/data

if [ ! -d "$WORK_DIR" ]; then
    mkdir "$WORK_DIR"
fi

if [ ! -d "$DATA_DIR" ]; then
    mkdir "$DATA_DIR"
fi

JOB_FILE=$THIS_DIR/dump_and_format.py
FILTER=$THIS_DIR/filter.json

START_DATE=`date +%Y%m%d -d "-9 months"`
# START_DATE=20140401
DATE_STRING="\"min\": \""$START_DATE"\""
sed "s/__DATES__/$DATE_STRING/" $THIS_DIR/$FILTER_TEMPLATE > $FILTER

echo "Job setup complete."
echo "Updating boto."

## Fix for BOTO.
sudo pip install -Iv boto==2.25.0 > $BOTO_LOG
# exec >> $LOG_FILE
echo "boto install complete."

echo "Running job." 

cd "$TELEMETRY_SERVER_DIR"

python -m mapreduce.job "$JOB_FILE" \
   --input-filter "$FILTER" \
   --num-mappers 16 \
   --num-reducers 4 \
   --work-dir "$WORK_DIR" \
   --data-dir "$DATA_DIR" \
   --output "$OUTPUT_FILE" \
   --bucket "telemetry-published-v2" \
   --verbose > $JOB_LOG

echo "Mapreduce job exited with code: $?"
echo "It is now `date`"
echo "Packaging output..."

cd "$BASE"
tar czf "$TARBALL" "`basename $OUTPUT_DIR`"
rm -f $OUTPUT_DIR/*
mv $TARBALL $OUTPUT_DIR

echo "Done. Exiting..."

exit 0


