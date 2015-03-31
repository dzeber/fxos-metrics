# Package necessary files to run processing on hala.
# Takes filename for tarball as parameter.

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
# $THIS_DIR is fxos-metrics/_older (as an absolute path).
# Base dir for fxos-metrics project is parent of $THIS_DIR.
BASE_DIR="$THIS_DIR/..".
PACKAGE_DIR="$HOME/tmp/package"

mkdir -p $PACKAGE_DIR
cd $PACKAGE_DIR

# Create directory structure. 
cp $BASE_DIR/processing/generate_datasets.sh .
cp $BASE_DIR/settings.env .
cp $BASE_DIR/processing/generate_datasets.py .
cp $BASE_DIR/shared/mapred.py .
cp $BASE_DIR/shared/ftu_formatter.py .
cp $BASE_DIR/shared/dump_schema.py .
cp $BASE_DIR/shared/formatting_rules.py .
cp $BASE_DIR/lookup/* .

# Permissions. 
chmod -R 755 generate_datasets.sh

# Package. 
cd ..
tar czvf "${1:-ftu-processing}.tar.gz" -C $PACKAGE_DIR .
rm -r $PACKAGE_DIR

exit 0

