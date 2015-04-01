# Package necessary files to run job on AWS cluster for uploading to S3.
# Requires filename for tarball as parameter.

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
# $THIS_DIR is fxos-metrics/_older (as an absolute path).
# Base dir for fxos-metrics project is parent of $THIS_DIR.
BASE_DIR="$THIS_DIR/..".
PACKAGE_DIR="$HOME/tmp/package"

mkdir -p $PACKAGE_DIR
cd $PACKAGE_DIR

# Create directory structure. 
cp $BASE_DIR/jobs/dump_format_ftu.py .
cp $BASE_DIR/shared/ftu_formatter.py .
cp $BASE_DIR/shared/mapred.py .
cp $BASE_DIR/shared/formatting_rules.py .
cp $BASE_DIR/shared/dump_schema.py .
cp $BASE_DIR/lookup/* .
cp $BASE_DIR/filters/all_fxos_date.json .
cp $BASE_DIR/dump_recent_ftu.sh .
cp $BASE_DIR/settings.env .
# Permissions. 
chmod -R 755 dump_recent_ftu.sh

# Package. 
cd ..
tar czvf "${1:-fxosping-dump-0.1}.tar.gz" -C $PACKAGE_DIR .
rm -r $PACKAGE_DIR

exit 0

