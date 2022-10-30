#!/bin/sh
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
  array=`echo $string | tr '=' ' '`
  case "${array[0]}" in
          IOTDB_INCLUDE)
               IOTDB_INCLUDE="${array[1]}"
          ;;
          IOTDB_CLI_CONF)
              IOTDB_CLI_CONF="${array[1]}"
          ;;
          *)
            #do nothing
          ;;
      esac
}

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME

PARAMETERS="$@"

# if [ $# -eq 0 ]
# then
# 	PARAMETERS="-h 127.0.0.1 -p 6667 -u root -pw root"
# fi

# Added parameters when default parameters are missing
user_param="-u root"
passwd_param="-pw root"
host_param="-h 127.0.0.1"
port_param="-p 6667"

while true; do
    case "$1" in
        -u)
            user_param="-u $2"
            shift 2
            ;;
        -pw)
            passwd_param="-p $2"
            shift 2
        ;;
        -h)
            host_param="-h $2"
            shift 2
        ;;
        -p)
            port_param="-p $2"
            shift 2
        ;;
        -D)
            checkEnvVaribles $2
            shift 2
        ;;
        -h)
            echo "Usage: $0 [-h <ip>] [-p <port>] [-u <username>] [-pw <password>] [-D <name=value>]"
            exit 0
        ;;
        "")
              #if we do not use getopt, we then have to process the case that there is no argument.
              shift
              break
              ;;
        *)
            #do nothing
        ;;
    esac
done

PARAMETERS="$host_param $port_param $user_param $passwd_param"

if [ -z "${IOTDB_INCLUDE}" ]; then
  #do nothing
  :
elif [ -r "$IOTDB_INCLUDE" ]; then
    . "$IOTDB_INCLUDE"
fi

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="`dirname "$0"`/.."
fi

if [ -z "${IOTDB_CLI_CONF}" ]; then
  IOTDB_CLI_CONF=${IOTDB_HOME}/conf
fi

MAIN_CLASS=org.apache.iotdb.cli.Cli


CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done


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



set -o noglob
iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-tool.xml"
exec "$JAVA" $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS

exit $?
