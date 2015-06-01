#!/bin/bash

# Pass as arguments jobscript filename (relative to awsjobs dir), 
# output filename, 
# and optionally filter filename relative to filter dir
# (default is no filter). 
# 
# Initial command-line option -l/--local will use local data if any.
# Command-line option --since <date> gives start date
# Command-line option --until <date> gives end date (default is none).
# Command-line option --ndays n gives the number of days.
# --ndays can be combined with either of the other two. 
# Option --reason can be used to specify the reason string (default is 'ftu').

# Earliest date to consider is 2014-04-01.
START_DATE_DEFAULT="20140401"
# Default filter file is template.
FILTER_TEMPLATE="all_fxos_date.json"

# Parse command-line options.

if [ $# -lt 2 ]; then
    echo "Usage: `basename $0` <opts> jobscript_name output_filename"
    echo "  (jobscript_name is relative to fxos-metrics/awsjobs dir)"
    echo "    --filter <filename> : use custom filter file in filters dir"
    echo "    --local : use local data, if any"
    echo "    --since <yyyy-mm-dd> : earliest date to include"
    echo "    --until <yyyy-mm-dd> : latest date to include"
    echo "    --ndays <n> : number of days to count"
    echo "    --reason : reason string ('ftu' or 'appusage' for FxOS)"
    exit 1
fi

while [ $# -gt 2 ]; do
    case "$1" in 
        -l|--local) 
            LOCAL="--local-only"
            ;;
        --since)
            shift
            START_DATE=`date +%Y%m%d -d "$1"`
            ;;
        --until)
            shift
            END_DATE=`date +%Y%m%d -d "$1"`
            ;;
        --ndays)
            shift
            NDAYS="$1"
            ;;
        --filter)
            shift
            FILTER_FILE="$1"
            ;;
        --reason)
            shift
            REASON_STRING="$1"
            ;;
        *)
            echo "Invalid option: $1"
            exit 1
            ;;
    esac
    shift
done

# Handle date options, if any.
if [ -n "$NDAYS" ]; then
    if [ -n "$START_DATE" ]; then
        # Can't have all three passed. 
        if [ -n "$END_DATE" ]; then
            echo "Can't use --since, --until, and --ndays all at the same time"
            exit 1
        fi
        # Set end date based on start date and difference.
        END_DATE=`date +%Y%m%d -d "$START_DATE+$NDAYS days"`
        [[ "$END_DATE" > `date +%Y%m%d` ]] && END_DATE=''
    else 
        # Backtrack from end date, if specified, otherwise from today.
        START_DATE=`date +%Y%m%d -d "$END_DATE-$NDAYS days"`
    fi
else
    # Use default start date, if unset.
    START_DATE=${START_DATE:-$START_DATE_DEFAULT}
    # End date will either be specified as an arg, or unset.
fi

CURRENT_DIR=$(pwd)
SRC_DIR=$(cd "`dirname "$0"`"; pwd)
TELEMETRY_SERVER_DIR=$HOME/telemetry-server

# Set up temporary storage.
WORK_DIR=/mnt/telemetry/work
DATA_CACHE=$WORK_DIR/cache

if [ ! -d "$WORK_DIR" ]; then
    mkdir "$WORK_DIR"
fi

if [ ! -d "$DATA_CACHE" ]; then
    mkdir "$DATA_CACHE"
fi

JOB_FILE=$SRC_DIR/awsjobs/$1
OUTPUT_FILE=$CURRENT_DIR/$2
# Path to the filter template that will be populated with custom values,
# if required.
FILTER_TEMPLATE=$SRC_DIR/awsjobs/filters/$FILTER_TEMPLATE
# Path to filter file that will be used in the job
# either a custom file or one generated from the template.
FILTER=$SRC_DIR/awsjobs/filters/${FILTER_FILE:-_date_filter.json}  

# Symlink utils dir into jobs dir so that telemetry-server job runner
# can see them. 
if [ ! -e $SRC_DIR/awsjobs/utils ]; then
    ln -s $SRC_DIR/utils $SRC_DIR/awsjobs/utils
fi

# Generate a filter file from the template, if required.
if [ -z "$FILTER_FILE" ]; then
    cp $FILTER_TEMPLATE $FILTER
    # Set the reason string.
    sed -i'' "s/__REASON__/${REASON_STRING:-ftu}/" $FILTER
    
    # Set the date range.
    DATE_STRING="\"min\": \""$START_DATE"\""
    if [ -n "$END_DATE" ]; then
        DATE_STRING="$DATE_STRING, \"max\": \""$END_DATE"\""
    fi
    sed -i'' "s/__DATES__/$DATE_STRING/" $FILTER
fi

echo "Running job $JOB_FILE with filter $FILTER"
echo "Dumping output to $OUTPUT_FILE"

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
