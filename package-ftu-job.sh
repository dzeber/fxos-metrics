# Package necessary files to run FTU job on AWS cluster.
# Creates a tarball in the current directory. 
# The filename for the tarball can be passed as an optional argument. 
# Default is "fxosping-dump-0.1.tar.gz".

# Base dir for fxos-metrics code.
BASE_DIR=$(cd "`dirname "$0"`"; pwd)
# Dir to create the tarball in.
TARGET_DIR=$(pwd)

cd $BASE_DIR

# Make sure runner has permissions. 
# chmod 755 dump_recent_ftu.sh

tar cvfz "$TARGET_DIR/${1:-fxosping-dump-0.1.tar.gz}" \
    # the job script
    awsjobs/dump_format_ftu.py \
    awsjobs/__init__.py \
    # the telemetry filter
    awsjobs/filters/all_fxos_date.json \
    # utils
    utils/*.py \
    # env variables
    settings.env \
    # runner script
    dump_recent_ftu.sh

exit 0

