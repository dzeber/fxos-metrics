#!/bin/bash

# Dump all FxOS records and download to hala.
# Pass remote instance hostname as argument.

AWS_HOST="ubuntu@$1"
SCRIPT_DIR=$(cd "`dirname "$0"`"; pwd)
JOB_TAR=jobfiles.tar.gz
OUTPUT_FILE="ftu_`date '+%Y-%m-%d'`.out"
LOCAL_DATA_DIR=$HOME/fxos/raw-data

# Package necessary files and send to node.
echo "Packaging job files..."

mkdir package
cp $SCRIPT_DIR/jobscripts/dump_all.py \
   $SCRIPT_DIR/filters/all_fxos.json \
   package   
tar cvzf $JOB_TAR -C package .
rm -rf package
scp $JOB_TAR "$AWS_HOST:/home/ubuntu/"
rm -f $JOB_TAR

echo "Running job..."

ssh "$AWS_HOST" \ 
    "tar xvfz $JOB_TAR;
    mkdir data work output; 
    sudo pip install -Iv boto==2.25.0; 
    cd telemetry-server;
    python -m mapreduce.job ~/dump_all.py \
       --input-filter ~/all_fxos.json \
       --num-mappers 16 \
       --num-reducers 4 \
       --work-dir ~/work \
       --data-dir ~/data \
       --output ~/$OUTPUT_FILE \
       --bucket telemetry-published-v2 \
       --verbose;
    gzip $OUTPUT_FILE;"

echo "Job completed. Downloading data..."
    
scp "$AWS_HOST:/home/ubuntu/$OUTPUT_FILE.gz" $LOCAL_DATA_DIR
gzip -d $LOCAL_DATA_DIR/$OUTPUT_FILE.gz

echo "Done." 
echo "--- Don't forget to kill the node!! ---"

exit 0
    

    