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

echo ----------------------------
echo Starting Standalone IoTDB
echo ----------------------------



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
            checkEnvVariables $2
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
            #SHOW_VERSION="yes"
            break
            echo "show version is not supported in current version on IoTDB"
            exit 1
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
  echo "Notice: in some systems, IoTDB must run in sudo mode to write data. The process may fail."
fi

#checkAllVariables is in iotdb-common.sh
checkAllConfigNodeVariables

#checkConfigNodePortUsages is in iotdb-common.sh
checkConfigNodePortUsages

#checkAllVariables is in iotdb-common.sh
checkAllVariables

#checkDataNodePortUsages is in iotdb-common.sh
checkDataNodePortUsages

PARAMS="-s $PARAMS"

#initEnv is in iotdb-common.sh
initEnv


CLASSPATH=""
for f in "${IOTDB_HOME}"/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.StandaloneIoTDB

launch_service() {
    class="$1"
    iotdb_parms="-Dlogback.configurationFile=${IOTDB_CONF}/logback-standalone.xml"
	  iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_HOME=${IOTDB_HOME}"
  	iotdb_parms="$iotdb_parms -DIOTDB_DATA_HOME=${IOTDB_DATA_HOME}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_DATA_HOME=${IOTDB_DATA_HOME}"
  	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
    iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_CONF=${IOTDB_CONF}"
  	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_HOME}"
  	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
    iotdb_parms="$iotdb_parms -DIOTDB_LOG_DIR=${IOTDB_LOG_DIR}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_LOGS=${IOTDB_LOG_DIR}"

  	  if [ "x$pidfile" != "x" ]; then
         iotdb_parms="$iotdb_parms -Diotdb-pidfile=$pidfile"
      fi

    # The iotdb-foreground option will tell IoTDB not to close stdout/stderr, but it's up to us not to background.
      if [ "x$foreground" == "xyes" ]; then
          iotdb_parms="$iotdb_parms -Diotdb-foreground=yes"
          if [ "x$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" != "x" ]; then
            [ -n "$pidfile" ] && printf "%d" $! > "$pidfile"
              # shellcheck disable=SC2154
              exec $NUMACTL "$JAVA" $JVM_OPTS "$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" $illegal_access_params $iotdb_parms $IOTDB_JVM_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS
          else
              [ -n "$pidfile" ] && printf "%d" $! > "$pidfile"
              exec $NUMACTL "$JAVA" $JVM_OPTS $illegal_access_params $iotdb_parms $IOTDB_JVM_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS
          fi
      # Startup IoTDB, background it, and write the pid.
      else
          if [ "x$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" != "x" ]; then
                exec $NUMACTL "$JAVA" $JVM_OPTS "$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" $illegal_access_params $iotdb_parms $IOTDB_JVM_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS 2>&1 > /dev/null  <&- &
                [ -n "$pidfile" ] && printf "%d" $! > "$pidfile"
                true
          else
                exec $NUMACTL "$JAVA" $JVM_OPTS $illegal_access_params $iotdb_parms $IOTDB_JVM_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS 2>&1 > /dev/null <&- &
                [ -n "$pidfile" ] && printf "%d" $! > "$pidfile"
                true
          fi
      fi

  	return $?

}

# Start up the service
launch_service "$classname"

exit $?
