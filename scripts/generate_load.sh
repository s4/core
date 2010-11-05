#!/bin/bash

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
  echo "  -a sender cluster name" >&2
  echo "  -g listener cluster name (same as -a if not specified)" >&2
  echo "  -z cluster manager address (hostname:port)" >&2
  echo "  -r emit rate" >&2
  echo "  -d rate display interval" >&2
  echo "  -s comma delimited list of schema files" >&2
  echo "  -u path to user defined event classes" >&2
  echo "  -x redbutton mode (explicit cluster management)" >&2
  echo "  -h help" >&2
  exit 1
fi

BASE_DIR=`dirname $($READLINK -f $0)`
CORE_HOME=`$READLINK -f ${BASE_DIR}/../s4_core`
CP_SEP=":"
REDBUTTON_MODE="false"

while getopts ":c:z:a:r:d:l:u:x" opt;
do  case "$opt" in
    c) CORE_HOME=$OPTARG;;
    z) CLUSTER_MANAGER=$OPTARG;;
    a) SENDER_CLUSTER_NAME=$OPTARG;;
    g) LISTENER_CLUSTER_NAME=$OPTARG;;
    r) RATE=$OPTARG;;
    d) DISPLAY_INTERVAL=$OPTARG;;
    l) LOCK_DIR=$OPTARG;;
    u) USER_CLASS_PATH=$OPTARG;;
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

INPUT_FILE=$1

if [ "x$CLUSTER_MANAGER" == "x" ] ; then
    CLUSTER_MANAGER="localhost:2181"
fi

if [ "x$SENDER_CLUSTER_NAME" == "x" ] ; then
    SENDER_CLUSTER_NAME="s4"
fi

if [ "x$LISTENER_CLUSTER_NAME" == "x" ] ; then
    LISTENER_CLUSTER_NAME=$SENDER_CLUSTER_NAME
fi

if [ "x$RATE" == "x" ] ; then
    RATE=80
fi

if [ "x$DISPLAY_INTERVAL" == "x" ] ; then
    DISPLAY_INTERVAL=15
fi

if [ "x$SCHEMA_FILE_LIST" != "x" ] ; then
    SCHEMA_FILE_LIST="${SCHEMA_FILE_LIST},"
fi
SCHEMA_FILE_LIST="${SCHEMA_FILE_LIST}${CORE_HOME}/conf/typical/schemas/EventWrapper_schema.js"

if [ "x$LOCK_DIR" == "x" ] ; then
    LOCK_DIR="${CORE_HOME}/lock"
fi

echo "Cluster manager ${CLUSTER_MANAGER}"
echo "Sender cluster name ${SENDER_CLUSTER_NAME}"
echo "Listener cluster name ${LISTENER_CLUSTER_NAME}"
echo "Rate ${RATE}"
echo "Display interval ${DISPLAY_INTERVAL}"
echo "Schema list ${SCHEMA_FILE_LIST}"

JAVA_LOC=""
if [ "x$JAVA_HOME" != "x" ] ; then
  JAVA_LOC=${JAVA_HOME}"/bin/"
fi

JAVA_OPTS=""
if [ "x$LOCK_DIR" != "x" ] ; then
  JAVA_OPTS="$JAVA_OPTS -Dlock_dir=$LOCK_DIR "
fi

echo "java location is ${JAVA_LOC}"
echo -n "JAVA VERSION="
echo `${JAVA_LOC}java -version`
#---------------------------------------------
#ADDING CORE JARS TO CLASSPATH
#---------------------------------------------

CLASSPATH=`find $CORE_HOME -name "*.jar" | awk '{p=$0"'$CP_SEP'"p;} END {print p}'`
if [ $REDBUTTON_MODE == "true" ] ; then
    CLASSPATH=${CLASSPATH}${CP_SEP}${CORE_HOME}/conf/redbutton
    JAVA_OPTS="$JAVA_OPTS -Dcommlayer.mode=static"
fi

if [ "x$USER_CLASS_PATH" != "x" ] ; then
    CLASSPATH=${CLASSPATH}${CP_SEP}${USER_CLASS_PATH}
fi


CMD="${JAVA_LOC}java $JAVA_OPTS -classpath $CLASSPATH io.s4.util.LoadGenerator -a ${SENDER_CLUSTER_NAME} -g ${LISTENER_CLUSTER_NAME} -z ${CLUSTER_MANAGER} -r${RATE} -d ${DISPLAY_INTERVAL} $INPUT_FILE"
echo "Running ${CMD}"
$CMD
