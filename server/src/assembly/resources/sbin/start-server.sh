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

# this function is for parsing the variables like "A=B" in  `start-server.sh -D A=B`
# The command just parse IOTDB-prefixed variables and ignore all other variables
checkEnvVaribles()
{
  string="$1"
  array=$(echo $string | tr '=' ' ')
  eval set -- "$array"
  case "$1" in
          IOTDB_INCLUDE)
               IOTDB_INCLUDE="$2"
               echo "iotdb include is $IOTDB_INCLUDE"
          ;;
          IOTDB_HOME)
               IOTDB_HOME="$2"
          ;;
          IOTDB_DATA_HOME)
             IOTDB_DATA_HOME="$2"
          ;;
          IOTDB_CONF)
             IOTDB_CONF="$2"
          ;;
          IOTDB_LOG_DIR)
             IOTDB_LOG_DIR="$2"
          ;;
          IOTDB_LOG_CONFIG)
             IOTDB_LOG_CONFIG="$2"
          ;;
          *)
            #do nothing
          ;;
      esac
}

# before, iotdb server runs on foreground by default
foreground="yes"

IOTDB_HEAP_DUMP_COMMAND=""

# Parse any command line options.
#args=`getopt gvRfbhp:c:bD::H:E: "$@"`
#eval set -- "$args"

while true; do
    case "$1" in
        -c)
            if [ ! -z "$2" ]; then
              if [ "${2:0:1}"  != "-" ]; then
                IOTDB_CONF="$2"
              else
                echo "Argument after -c cannot starts with '-', exiting"
                exit 0
              fi
            else
              echo "Missing argument after -c, exiting"
              exit 0
            fi
            shift 2
        ;;
        -p)
            if [ ! -z "$2" ]; then
              if [ "${2:0:1}"  != "-" ]; then
                pidfile="$2"
              else
                echo "Argument after -p cannot starts with '-', exiting"
                exit 0
              fi
            else
              echo "Missing argument after -p, exiting"
              exit 0
            fi
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
            echo "check....$1 and $2"
            checkEnvVaribles $2
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
        printgc)
            PRINT_GC="yes"
            shift
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

if [ -z "${IOTDB_INCLUDE}" ]; then
  #do nothing
  :
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

if [ -z "${IOTDB_LOG_CONFIG}" ]; then
  export IOTDB_LOG_CONFIG="${IOTDB_CONF}/logback.xml"
fi


if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" >/dev/null 2>&1; then
  echo "IoTDB already exists! Quiting"
  echo Pid = "$(cat "$pidfile")"
  exit 0
fi

CLASSPATH=""
for f in "${IOTDB_HOME}"/lib/*.jar; do
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

if [ -f "$IOTDB_CONF/iotdb-env.sh" ]; then
    if [ "x$PRINT_GC" != "x" ]; then
      . "$IOTDB_CONF/iotdb-env.sh" "printgc"
    else
        . "$IOTDB_CONF/iotdb-env.sh"
    fi
else
    echo "Can't find $IOTDB_CONF/iotdb-env.sh"
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

  if [ "x$pidfile" != "x" ]; then
     iotdb_parms="$iotdb_parms -Diotdb-pidfile=$pidfile"
  fi

  # The iotdb-foreground option will tell IoTDB not to close stdout/stderr, but it's up to us not to background.
  if [ "x$foreground" == "xyes" ]; then
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