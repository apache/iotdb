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

if [ "x$IOTDB_INCLUDE" = "x" ]; then
    # Locations (in order) to use when searching for an include file.
    for include in "`dirname "$0"`/iotdb.in.sh" \
                   "$HOME/.iotdb.in.sh" \
                   /usr/share/iotdb/iotdb.in.sh \
                   /etc/iotdb/iotdb.in.sh \
                   /opt/iotdb/iotdb.in.sh; do
        if [ -r "$include" ]; then
            . "$include"
            break
        fi
    done
# ...otherwise, source the specified include.
elif [ -r "$IOTDB_INCLUDE" ]; then
    . "$IOTDB_INCLUDE"
fi

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="`dirname "$0"`/.."
fi

if [ -z "${IOTDB_DATA_HOME}" ]; then
  export IOTDB_DATA_HOME=${IOTDB_HOME}
fi

if [ -z "${IOTDB_CONF}" ]; then
  export IOTDB_CONF=${IOTDB_HOME}/conf
fi

if [ -z "${IOTDB_LOG_DIR}" ]; then
  export IOTDB_LOG_DIR=${IOTDB_HOME}/logs
fi

if [ -z "${configurationFile}" ]; then
  IOTDB_LOG_CONFIG="${IOTDB_CONF}/logback.xml"
fi

# before, iotdb server runs on foreground by default
foreground="yes"

IOTDB_HEAP_DUMP_COMMAND=""

# Parse any command line options.
#args=`getopt gvRfbhp:c:bD::H:E: "$@"`
#eval set -- "$args"

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
        -b)
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
            shift 2
        ;;
        -X)
            IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -XX:$2"
            shift 2
        ;;
        -h)
            echo "Usage: $0 [-v] [-f] [-b] [-h] [-p pidfile] [-c configFolder] [-H HeapDumpPath] [-E JvmErrorFile] [printgc]"
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
            shift
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

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

classname=org.apache.iotdb.db.service.IoTDB

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
echo Starting IoTDB
echo ---------------------

## this is for being compatibile with v0.13, can be removed from v0.14 on.
data=($*)
if [ "x${data[0]}" == "xprintgc" ]; then
  PRINT_GC="yes"
fi
## end

if [ -f "$IOTDB_CONF/iotdb-env.sh" ]; then
    if [ "x$PRINT_GC" != "x" ]; then
      . "$IOTDB_CONF/iotdb-env.sh" "printgc"
    else
        . "$IOTDB_CONF/iotdb-env.sh"
    fi
else
    echo "can't find $IOTDB_CONF/iotdb-env.sh"
fi

# check whether we can enable heap dump when oom
if [ "x$IOTDB_ALLOW_HEAP_DUMP" == "xtrue" ]; then
  IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS $IOTDB_HEAP_DUMP_COMMAND"
fi

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

  if [ "x$pidpath" != "x" ]; then
     iotdb_parms="$iotdb_parms -Diotdb-pidfile=$pidpath"
  fi

  # The iotdb-foreground option will tell IoTDB not to close stdout/stderr, but it's up to us not to background.
  if [ "x$foreground" == "x" ]; then
      iotdb_parms="$iotdb_parms -Diotdb-foreground=yes"
      if [ "x$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" != "x" ]; then
          exec $NUMACTL "$JAVA" $JVM_OPTS "$JVM_ON_OUT_OF_MEMORY_ERROR_OPT" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" $IOTDB_JVM_OPTS "$class" $PARAMS
      else
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

# Start up the service
launch_service "$classname"

exit $?
