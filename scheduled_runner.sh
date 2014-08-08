#!/bin/bash

## Run scheduled job to download latest FTU ping data.

BASE=$(pwd)
THIS_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$HOME/telemetry-server

OUTPUT_DIR=$BASE/output
OUTPUT_FILE=$OUTPUT_DIR/ftu_data.out
LOG_FILE=$OUTPUT_DIR/ftu_job.log
BOTO_LOG=$OUTPUT_DIR/boto.log
JOB_LOG=$OUTPUT_DIR/job.log

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

JOB_FILE=$THIS_DIR/jobscripts/count_activations.py
FILTER=$THIS_DIR/filters/all_fxos.json

echo "Job setup complete."
echo "Updating boto."
# exec 1>&-
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

exit 0
