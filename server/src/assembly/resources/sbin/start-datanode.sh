#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


source "$(dirname "$0")/iotdb-common.sh"

# iotdb server runs on foreground by default
foreground="yes"

IOTDB_HEAP_DUMP_COMMAND=""

if [ $# -ne 0 ]; then
  echo "All parameters are $*"
fi

while true; do
    case "$1" in
        -c)
            IOTDB_CONF="$2"
            shift 2
            ;;
        -p)
            pidfile="$2"
            shift 2
        ;;
        -f)
            foreground="yes"
            shift
        ;;
        -d)
            foreground=""
            shift
        ;;
        -g)
            PRINT_GC="yes"
            shift
        ;;
        -H)
            IOTDB_HEAP_DUMP_COMMAND="$IOTDB_HEAP_DUMP_COMMAND -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$2"
            shift 2
        ;;
        -E)
            IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -XX:ErrorFile=$2"
            shift 2
        ;;
        -D)
            IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -D$2"
            #checkEnvVariables is in iotdb-common.sh
            checkEnvVariables "$2"
            shift 2
        ;;
        -X)
            IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -XX:$2"
            shift 2
        ;;
        -h)
            echo "Usage: $0 [-v] [-f] [-d] [-h] [-p pidfile] [-c configFolder] [-H HeapDumpPath] [-E JvmErrorFile] [printgc]"
            exit 0
        ;;
        -v)
            SHOW_VERSION="yes"
            break
        ;;
        --)
            shift
            #all others are args to the program
            PARAMS=$*
            break
        ;;
        "")
            #if we do not use getopt, we then have to process the case that there is no argument.
            #in some systems, when there is no argument, shift command may throw error, so we skip directly
            #all others are args to the program
            PARAMS=$*
            break
        ;;
        *)
            echo "Error parsing arguments! Unknown argument \"$1\"" >&2
            exit 1
        ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
  echo "Notice: in some systems, DataNode must run in sudo mode to write data. The process may fail."
fi

#checkAllVariables is in iotdb-common.sh
checkAllVariables

#checkDataNodePortUsages is in iotdb-common.sh
checkDataNodePortUsages

CLASSPATH=""
for f in "${IOTDB_HOME}"/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

classname=org.apache.iotdb.db.service.DataNode


if [ "x$SHOW_VERSION" != "x" ]; then
    classname=org.apache.iotdb.db.service.GetVersion
    IOTDB_LOG_CONFIG="${IOTDB_CONF}/logback-tool.xml"
    # find java in JAVA_HOME
    if [ -n "$JAVA_HOME" ]; then
        for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
            if [ -x "$java" ]; then
                JAVA="$java"
                break
            fi
        done
    else
        JAVA=java
    fi
    exec "$JAVA" -cp "$CLASSPATH" $IOTDB_JVM_OPTS "-Dlogback.configurationFile=${IOTDB_LOG_CONFIG}" "$classname"
    exit 0
fi

echo ---------------------
echo "Starting IoTDB DataNode"
echo ---------------------

#initEnv is in iotdb-common.sh
initEnv

# check whether we can enable heap dump when oom
if [ "x$IOTDB_ALLOW_HEAP_DUMP" == "xtrue" ]; then
  IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS $IOTDB_HEAP_DUMP_COMMAND"
fi

# -s means start a data node. -r means remove a data node.

PARAMS="-s $PARAMS"

classname=org.apache.iotdb.db.service.DataNode

launch_service()
{
	class="$1"
  iotdb_parms="-Dlogback.configurationFile=${IOTDB_LOG_CONFIG}"
	iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_DATA_HOME=${IOTDB_DATA_HOME}"
	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
	iotdb_parms="$iotdb_parms -DIOTDB_LOG_DIR=${IOTDB_LOG_DIR}"

	  if [ "x$pidfile" != "x" ]; then
       iotdb_parms="$iotdb_parms -Diotdb-pidfile=$pidfile"
    fi

  # The iotdb-foreground option will tell IoTDB not to close stdout/stderr, but it's up to us not to background.
    if [ "x$foreground" == "xyes" ]; then
        iotdb_parms="$iotdb_parms -Diotdb-foreground=yes"
        if [ "x$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" != "x" ]; then
          [ ! -z "$pidfile" ] && printf "%d" $! > "$pidfile"
            exec $NUMACTL "$JAVA" $JVM_OPTS "$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS
        else
            [ ! -z "$pidfile" ] && printf "%d" $! > "$pidfile"
            exec $NUMACTL "$JAVA" $JVM_OPTS $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS
        fi
    # Startup IoTDB, background it, and write the pid.
    else
        if [ "x$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" != "x" ]; then
              exec $NUMACTL "$JAVA" $JVM_OPTS "$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS 2>&1 > /dev/null  <&- &
              [ ! -z "$pidfile" ] && printf "%d" $! > "$pidfile"
              true
        else
              exec $NUMACTL "$JAVA" $JVM_OPTS $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS 2>&1 > /dev/null <&- &
              [ ! -z "$pidfile" ] && printf "%d" $! > "$pidfile"
              true
        fi
    fi

	return $?
}


# check whether tool 'lsof' exists
check_tool_env() {
  if  ! type lsof > /dev/null 2>&1 ; then
    echo ""
    echo " Warning: No tool 'lsof', Please install it."
    echo " Note: Some checking function need 'lsof'."
    echo ""
    return 1
  else
    return 0
  fi
}

# convert path to real full-path.
# e.g., /a/b/c/.. will return /a/b
# If path has been deleted, return ""
get_real_path() {
  local path=$1
  local real_path=""
  cd $path > /dev/null 2>&1
  if [ $? -eq 0 ] ; then
    real_path=$(pwd -P)
    cd -  > /dev/null 2>&1
  fi
  echo "${real_path}"
}

# check whether same directory's IoTDB node process has been running
check_running_process() {
  check_tool_env

  PIDS=$(ps ax | grep "$classname" | grep java | grep DIOTDB_HOME | grep -v grep | awk '{print $1}')
  for pid in ${PIDS}
  do
    run_conf_path=""
    # find the abstract path of the process
    run_cwd=$(lsof -p $pid 2>/dev/null | awk '$4~/cwd/ {print $NF}')
    # find "-DIOTDB_HOME=XXX" from the process command
    run_home_path=$(ps -fp $pid | sed "s/ /\n/g" | sed -n "s/-DIOTDB_HOME=//p")
    run_home_path=$(get_real_path "${run_cwd}/${run_home_path}")

    #if dir ${run_home_path} has been deleted
    if [ "${run_home_path}" == "" ]; then
      continue
    fi

    current_home_path=$(get_real_path ${IOTDB_HOME})
    if [ "${run_home_path}" == "${current_home_path}" ]; then
      echo ""
      echo " Found running IoTDB node (PID=$pid)."  >&2
      echo " Can not run duplicated IoTDB node!"  >&2
      echo " Exit..."  >&2
      echo ""
      exit 1
    fi
  done
}


check_tool_env
# If needed tool is ready, check whether same directory's IoTDB node is running
if [ $? -eq 0 ]; then
  check_running_process
fi

# Start up the service
launch_service "$classname"

exit $?
