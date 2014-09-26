# Package necessary files to run job on AWS cluster for uploading to S3.
# Requires filename for tarball as parameter.

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
PACKAGE_DIR="$HOME/tmp/package"

mkdir -p $PACKAGE_DIR
cd $PACKAGE_DIR

# Create directory structure. 
cp $THIS_DIR/jobs/ftu_dump/dump_and_format.py .
cp $THIS_DIR/shared/ftu_formatter.py .
cp $THIS_DIR/shared/mapred.py .
cp $THIS_DIR/shared/formatting_rules.py .
cp $THIS_DIR/lookup/* .
cp $THIS_DIR/filters/all_fxos_date.json .
cp $THIS_DIR/jobs/ftu_dump/dump_records.sh .
cp $THIS_DIR/settings.env .
# Permissions. 
chmod -R 755 dump_records.sh

# Package. 
cd ..
tar czvf "${1:-fxosping-dump-0.1}.tar.gz" -C $PACKAGE_DIR .
rm -r $PACKAGE_DIR

exit 0

