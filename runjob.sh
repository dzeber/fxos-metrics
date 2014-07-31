#!/bin/bash

# Pass as arguments jobscript filename, output filename, and optionally filter filename (default is no filter). 
# Initial command-line option -l/--local will use local data if any.

LOCAL=""
if [ "$1" == "-l" -o "$1" == "--local" ]; then
    LOCAL="--local-only"
    shift
fi

BASE=$(pwd)
THIS_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$HOME/telemetry-server

# OUTPUT=${OUTPUT:-output}
#TODAY=$(date +%Y%m%d)

# if [ ! -d "$OUTPUT" ]; then
    # mkdir -p "$OUTPUT"
# fi

# if [ ! -d "job" ]; then
    # mkdir -p "job"
# fi

WORK_DIR=$BASE/work
DATA_CACHE=$WORK_DIR/cache

if [ ! -d "$WORK_DIR" ]; then
    mkdir "$WORK_DIR"
fi

if [ ! -d "$DATA_CACHE" ]; then
    mkdir "$DATA_CACHE"
fi

# if [ ! -d "data" ]; then
    # mkdir -p "data"
# fi

JOB_FILE=$THIS_DIR/jobscripts/$1
# OUTPUT_FILE=$BASE/$OUTPUT/$2
OUTPUT_FILE=$BASE/$2
FILTER=$THIS_DIR/filters/${3:-all_fxos.json}  

echo "Running job $JOB_FILE with filter $FILTER"
echo "Dumping output to $OUTPUT_FILE"

# If we have an argument, process that day.
# TARGET=$1
# if [ -z "$TARGET" ]; then
    # Default to processing "yesterday"
    # TARGET=$(date -d 'yesterday' +%Y%m%d)
# fi

# if [ "$TARGET" = "all" ]; then
    # TARGET_DATE="\"*\""
# else
    # TARGET_DATE="[\"$TARGET\"]"
# fi

# echo "Today is $TODAY, and we're gathering fxosping data for '$TARGET'"

# sed -r "s/__TARGET_DATE__/$TARGET_DATE/" \
       # "$THIS_DIR/filter_template.json" > "$THIS_DIR/filter.json"

cd "$TELEMETRY_SERVER_DIR"

echo "Starting fxosping export" 

python -m mapreduce.job "$JOB_FILE" \
   --input-filter "$FILTER" \
   --num-mappers 16 \
   --num-reducers 4 \
   --work-dir "$WORK_DIR" \
   --data-dir "$DATA_CACHE" \
   $LOCAL \
   --output "$OUTPUT_FILE" \
   --bucket "telemetry-published-v2" \
   --verbose

echo "Mapreduce job exited with code: $?"

echo "Output is located in $OUTPUT_FILE"
