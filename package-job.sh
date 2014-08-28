# Package necessary files to run job on AWS cluster for uploading to S3.
# Requires filename for tarball as parameter.

THIS_DIR=$(cd "`dirname "$0"`"; pwd)
PACKAGE_DIR="$HOME/tmp/package"

mkdir -p $PACKAGE_DIR
cd $PACKAGE_DIR

# Create directory structure. 
mkdir jobscripts filters lookup
cp $THIS_DIR/jobscripts/count_activations.py jobscripts
cp $THIS_DIR/filters/all_fxos.json filters
cp $THIS_DIR/lookup/* lookup
cp $THIS_DIR/scheduled_runner.sh .
cp $THIS_DIR/settings.env
# Permissions. 
chmod -R 755 .

# Package. 
cd ..
tar czvf "${1:-fxosping-0.2}.tar.gz" -C $PACKAGE_DIR .
rm -r $PACKAGE_DIR

exit 0

