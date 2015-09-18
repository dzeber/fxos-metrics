#!/bin/bash

# Run R script and copy outputs to dashboard1.

. ~/.bash_profile
. /etc/profile.d/mozilla.sh

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
cd ~/fxos-data/au/foxfood
exec >> job.log 2>&1

Rscript --vanilla $THIS_DIR/postprocessing/inactive_foxfooders.R
DEST_DIR=$(ssh $WWW ". .bash_profile; echo \$WWW")
DEST_DIR="$DEST_DIR/dzeber/foxfooding"
scp unactivated_foxfooders.txt inactive_foxfooders.csv $WWW:$DEST_DIR
