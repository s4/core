#/bin/bash

get_property()
{
  val=`sed '/^\#/d' ${CONF_LOC}/s4_core.properties_header | grep $1  | tail -n 1 | sed 's/^[^=]*=//;s/^[[:space:]]*//;s/[[:space:]]*$//'`
  echo "$val"
}

osx=false
case "`uname`" in
Darwin*) osx=true;;
esac

if $osx; then
    READLINK="stat"    
else
    READLINK="readlink"
fi

#---------------------------------------------
# USAGE and read arguments
#---------------------------------------------

if [ "$1" == "-h" ]; then
  echo "Usage: $0" >&2
  echo "  -c s4 core home" >&2
  echo "  -a s4 apps home" >&2
  echo "  -s sender cluster name" >&2
  echo "  -g listener cluster name (same as -a if not specified)" >&2
  echo "  -z cluster manager address (hostname:port)" >&2
  echo "  -u path to user defined data adapter classes" >&2
  echo "  -x redbutton mode (explicit cluster management)" >&2
  echo "  -d legacy data adapter conf file" >&2
  echo "  -h help" >&2
  exit 1
fi

BASE_DIR=`dirname $($READLINK -f $0)`
CORE_HOME=`$READLINK -f ${BASE_DIR}/../s4_core`
APPS_HOME=`$READLINK -f ${BASE_DIR}/../s4_apps`
CP_SEP=":"
REDBUTTON_MODE="false"

while getopts ":c:z:a:s:g:l:u:d:x" opt;
do  case "$opt" in
    c) CORE_HOME=$OPTARG;;
    a) APPS_HOME=$OPTARG;;
    z) CLUSTER_MANAGER=$OPTARG;;
    s) SENDER_CLUSTER_NAME=$OPTARG;;
    g) LISTENER_CLUSTER_NAME=$OPTARG;;
    l) LOCK_DIR=$OPTARG;;
    u) USER_CLASS_PATH=$OPTARG;;
    d) DATA_ADAPTER_CONF=$OPTARG;;
    x) REDBUTTON_MODE="true";;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
     :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    esac
done
shift $(($OPTIND-1))

CONF_TYPE=$1
if [ "x$CONF_TYPE" == "x" ] ; then
    CONF_TYPE="redbutton"
fi

CONF_FILE=${CORE_HOME}"/conf/"${CONF_TYPE}"/adapter_conf.xml"
CONF_LOC=`dirname $CONF_FILE`
LOG_LOC="${CORE_HOME}/logs"
COMMLAYER_MODE=$(get_property "commlayer_mode")

if [ "x$CLUSTER_MANAGER" == "x" ] ; then
    CLUSTER_MANAGER="localhost:2181"
fi

if [ "x$SENDER_CLUSTER_NAME" == "x" ] ; then
    SENDER_CLUSTER_NAME="s4"
fi

if [ "x$LISTENER_CLUSTER_NAME" == "x" ] ; then
    LISTENER_CLUSTER_NAME=$SENDER_CLUSTER_NAME
fi

if [ "x$LOCK_DIR" == "x" ] ; then
    LOCK_DIR="${CORE_HOME}/lock"
fi

if [ "x$DATA_ADAPTER_CONF" == "x" ] ; then
    echo "No data adapter configuration specified"
    exit 1
fi

echo "Cluster manager ${CLUSTER_MANAGER}"
echo "Sender cluster name ${SENDER_CLUSTER_NAME}"
echo "Listener cluster name ${LISTENER_CLUSTER_NAME}"

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

JAVA_OPTS=""
if [ "x$LOCK_DIR" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dlock_dir=$LOCK_DIR "
fi

JAVA_OPTS="$JAVA_OPTS -Dlog_loc=${LOG_LOC} "

echo "java location is ${JAVA_LOC}"
echo -n "JAVA VERSION="
echo `${JAVA_LOC}java -version`
#---------------------------------------------
#ADDING CORE JARS TO CLASSPATH
#---------------------------------------------

CLASSPATH=`find $CORE_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
CLASSPATH=$CLASSPATH$CP_SEP`find $APPS_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
JAVA_OPTS="$JAVA_OPTS -Dzk_session_timeout=5000"

CLASSPATH=${CLASSPATH}${CP_SEP}${CONF_LOC}
if [ $REDBUTTON_MODE == "true" ] ; then
    JAVA_OPTS="$JAVA_OPTS -Dcommlayer.mode=static"
else
    JAVA_OPTS="$JAVA_OPTS -Dcommlayer.mode=${COMMLAYER_MODE}"
fi

if [ "x$USER_CLASS_PATH" != "x" ] ; then
    CLASSPATH=${CLASSPATH}${CP_SEP}${USER_CLASS_PATH}
fi

MKTEMP_ARGS=""

if $osx ; then
    MKTEMP_ARGS="tmpXXXX" 
fi

TMP1=`mktemp -d $MKTEMP_ARGS`
echo "Temp is $TMP1"
echo "appName=${SENDER_CLUSTER_NAME}" > $TMP1/adapter.properties
echo "listenerAppName=${LISTENER_CLUSTER_NAME}" >> $TMP1/adapter.properties
echo "listener_max_queue_size=8000" >> $TMP1/adapter.properties
cat $TMP1/adapter.properties

CLASSPATH=${CLASSPATH}${CP_SEP}${TMP1}

CMD="${JAVA_LOC}java $JAVA_OPTS -Dlog4j.configuration=file:${CONF_LOC}/log4j.xml -classpath $CLASSPATH io.s4.client.Adapter -t ${CONF_TYPE} -c ${CORE_HOME} -d ${DATA_ADAPTER_CONF}"
echo "Running ${CMD}"
$CMD
