# Package necessary files to run processing on hala.
# Takes filename for tarball as parameter.

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
PACKAGE_DIR="$HOME/tmp/package"

mkdir -p $PACKAGE_DIR
cd $PACKAGE_DIR

# Create directory structure. 
cp $THIS_DIR/processing/generate_datasets.sh .
cp $THIS_DIR/settings.env .
cp $THIS_DIR/processing/generate_datasets.py .
cp $THIS_DIR/shared/mapred.py .
cp $THIS_DIR/shared/ftu_formatter.py .
cp $THIS_DIR/shared/dump_schema.py .
cp $THIS_DIR/shared/formatting_rules.py .
cp $THIS_DIR/lookup/* .

# Permissions. 
chmod -R 755 generate_datasets.sh

# Package. 
cd ..
tar czvf "${1:-ftu-processing}.tar.gz" -C $PACKAGE_DIR .
rm -r $PACKAGE_DIR

exit 0

