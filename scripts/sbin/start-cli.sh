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

# DEFAULT_SQL_DIALECT is used to set the default SQL dialect for the CLI.
# empty value means using "tree".
# Optional values: "table" or "tree"
DEFAULT_SQL_DIALECT=

# You can put your env variable here
# export JAVA_HOME=$JAVA_HOME


# this function is for parsing the variables like "A=B" in  `start-server.sh -D A=B`
# The command just parse IOTDB-prefixed variables and ignore all other variables
checkEnvVariables()
{
  string="$1"
  array=$(echo $string | tr '=' ' ')
  eval set -- "$array"
  case "$1" in
          IOTDB_INCLUDE)
               IOTDB_INCLUDE="$2"
          ;;
          IOTDB_CLI_CONF)
              IOTDB_CLI_CONF="$2"
          ;;
          *)
            #do nothing
          ;;
      esac
}

PARAMETERS=""

# if [ $# -eq 0 ]
# then
# 	PARAMETERS="-h 127.0.0.1 -p 6667 -u root -pw root"
# fi

# if DEFAULT_SQL_DIALECT is empty, set it to "tree"
if [ -z "$DEFAULT_SQL_DIALECT" ]; then
    DEFAULT_SQL_DIALECT="tree"
fi

# Added parameters when default parameters are missing
user_param="-u root"
passwd_param="-pw root"
host_param="-h 127.0.0.1"
port_param="-p 6667"
sql_dialect_param="-sql_dialect $DEFAULT_SQL_DIALECT"

while true; do
    case "$1" in
        -u)
            user_param="-u $2"
            shift 2
            ;;
        -pw)
            if [ -n "$2" ] && [[ ! "$2" =~ ^- ]]; then
                passwd_param="-pw $2"
                shift 2
            else
                passwd_param="-pw"
                shift
            fi
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
            checkEnvVariables $2
            shift 2
        ;;
        -sql_dialect)
            sql_dialect_param="-sql_dialect $2"
            shift 2
        ;;
        "")
              #if we do not use getopt, we then have to process the case that there is no argument.
              #in some systems, when there is no argument, shift command may throw error, so we skip directly
              break
              ;;
        *)
            PARAMETERS="$PARAMETERS $1"
            shift
        ;;
    esac
done

PARAMETERS="$host_param $port_param $user_param $passwd_param $sql_dialect_param $PARAMETERS"

if [ -z "${IOTDB_INCLUDE}" ]; then
  #do nothing
  :
elif [ -r "$IOTDB_INCLUDE" ]; then
    . "$IOTDB_INCLUDE"
fi

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

IOTDB_CLI_CONF=${IOTDB_HOME}/conf

MAIN_CLASS=org.apache.iotdb.cli.Cli

CLASSPATH=${IOTDB_HOME}/lib/*


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

# Determine the sort of JVM we'll be running on.
java_ver_output=`"$JAVA" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}' | cut -d\- -f1`
JVM_VERSION=${jvmver%_*}

version_arr=(${JVM_VERSION//./ })

illegal_access_params=""
#GC log path has to be defined here because it needs to access IOTDB_HOME
if [ "${version_arr[0]}" = "1" ] ; then
    # Java 8
    MAJOR_VERSION=${version_arr[1]}
else
    #JDK 11 and others
    MAJOR_VERSION=${version_arr[0]}
    # Add argLine for Java 11 and above, due to [JEP 396: Strongly Encapsulate JDK Internals by Default] (https://openjdk.java.net/jeps/396)
    illegal_access_params="$illegal_access_params --add-opens=java.base/java.lang=ALL-UNNAMED"
fi

JVM_OPTS="-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

set -o noglob
iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-cli.xml"
exec "$JAVA" $JVM_OPTS $iotdb_cli_params $illegal_access_params -cp "$CLASSPATH" "$MAIN_CLASS" $PARAMETERS

exit $?
