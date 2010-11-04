
if [ "x$1" == "x" ]; then
    echo "No target image directory specified"
    exit 1
fi

rm -fr $1/bin
rm -fr $1/s4_core
rm -fr $1/s4_apps
rm -fr $1/s4_exts
rm -fr $1/tools

# An image directory has this structure: 
#    <image_dir>/s4_core
#    <image_dir>/s4_core/bin
#    <image_dir>/s4_core/conf/typical
#    <image_dir>/s4_core/conf/redbutton
#    <image_dir>/s4_core/jars
#    <image_dir>/s4_core/lock
#    <image_dir>/s4_core/logs
#    <image_dir>/s4_apps

mkdir $1/bin
mkdir $1/s4_core
mkdir $1/s4_core/lib
mkdir $1/s4_core/lock
mkdir $1/s4_core/logs
mkdir $1/s4_core/conf
mkdir $1/s4_core/conf/typical
mkdir $1/s4_core/conf/redbutton
mkdir $1/s4_core/conf/redbutton/s4
mkdir $1/s4_apps
mkdir $1/s4_exts
mkdir $1/tools
mkdir $1/tools/load_generator
mkdir $1/tools/load_generator/lib

cp ../target/s4_core-*.dir/s4_core-*.jar $1/s4_core/lib
cp ../target/s4_core-*.dir/lib/*.jar $1/s4_core/lib
cp ../target/s4_core-*.dir/s4_core_conf_typical.xml $1/s4_core/conf/typical/s4_core_conf.xml
cp ../target/s4_core-*.dir/adapter_conf.xml $1/s4_core/conf/typical/adapter_conf.xml
cp ../target/s4_core-*.dir/log4j.xml $1/s4_core/conf/typical/log4j.xml
cp ../target/s4_core-*.dir/s4_core.properties_header_typical $1/s4_core/conf/typical/s4_core.properties_header
cp -r ../target/s4_core-*.dir/schemas $1/s4_core/conf/typical/

# for now, use typical conf file in redbutton mode
cp ../target/s4_core-*.dir/s4_core_conf_typical.xml $1/s4_core/conf/redbutton/s4_core_conf.xml
cp ../target/s4_core-*.dir/adapter_conf.xml $1/s4_core/conf/redbutton/adapter_conf.xml
cp ../target/s4_core-*.dir/log4j.xml $1/s4_core/conf/redbutton/log4j.xml
cp ../target/s4_core-*.dir/s4_core.properties_header_redbutton $1/s4_core/conf/redbutton/s4_core.properties_header
cp -r ../target/s4_core-*.dir/schemas $1/s4_core/conf/redbutton/
cp ../target/s4_core-*.dir/sender.xml $1/s4_core/conf/redbutton/s4
cp ../target/s4_core-*.dir/listener.xml $1/s4_core/conf/redbutton/s4

cp s4_start.sh $1/bin
cp generate_load.sh $1/bin
cp run_adapter.sh $1/bin

