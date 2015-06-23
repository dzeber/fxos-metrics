# Package necessary files to run AU job on AWS cluster.
# Creates a tarball in the current directory. 
# The filename for the tarball can be passed as an optional argument. 
# Default is "au-dump-0.1.tar.gz".

# Base dir for fxos-metrics code.
BASE_DIR=$(cd "`dirname "$0"`"; pwd)
# Dir to create the tarball in.
TARGET_DIR=$(pwd)

cd $BASE_DIR

# Run AWS job from a flatter configuration. 
# Add symlinks to flatten structure when archiving.
ln -s $BASE_DIR/awsjobs/dump_format_appusage.py $BASE_DIR/dump_format_appusage.py
ln -s $BASE_DIR/awsjobs/filters/all_fxos_date.json $BASE_DIR/all_fxos_date.json

# The utils dir needs to be inside the jobs dir to the job to run correctly.
# Create a symlink to be followed when archiving.
#ln -s $BASE_DIR/utils $BASE_DIR/awsjobs/utils

tar cvfz "$TARGET_DIR/${1:-au-dump-0.1.tar.gz}" -h \
    dump_format_appusage.py \
    all_fxos_date.json \
    utils/*.py \
    utils/lookup \
    dump_appusage.sh
    
# Remove symlink. 
unlink $BASE_DIR/dump_format_appusage.py
unlink $BASE_DIR/all_fxos_date.json

exit 0

