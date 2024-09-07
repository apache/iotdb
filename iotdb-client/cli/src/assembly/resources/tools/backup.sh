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

echo ------------------------------------------
echo Starting IoTDB Client Data Back Script
echo ------------------------------------------


if [ -z "${IOTDB_INCLUDE}" ]; then
  #do nothing
  :
elif [ -r "$IOTDB_INCLUDE" ]; then
    . "$IOTDB_INCLUDE"
fi

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

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

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables.  > /dev/stderr
    exit 1;
fi

datanodeclassname=org.apache.iotdb.db.service.DataNode

confignodeclassname=org.apache.iotdb.db.service.ConfigNode

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

get_properties_value() {
    local file_name=$1
    local property_name=$2
    local default_value=$3
    if [ -f "${IOTDB_HOME}/conf/iotdb-system.properties" ]; then
            local file_path="${IOTDB_HOME}/conf/iotdb-system.properties"
    else
            local file_path="${IOTDB_HOME}/conf/${file_name}.properties"
    fi
    local property_value=$(sed "/^${property_name}=/!d;s/.*=//" "${file_path}")
    if [ -z "$property_value" ]; then
            property_value="$default_value"
    fi
    echo "$property_value"
}
iotdb_listening_ports=()
check_running_process() {
    DATANODE="iotdb-datanode"
    CONFIGNODE="iotdb-confignode"
    dn_rpc_port=$(get_properties_value $DATANODE "dn_rpc_port" "6667")
    cn_internal_port=$(get_properties_value $CONFIGNODE "cn_internal_port" "10710")
    local_ports+=("$dn_rpc_port")
    local_ports+=("$cn_internal_port")
    for port in "${local_ports[@]}"; do
      # Check if lsof command is available
      if command -v lsof >/dev/null 2>&1; then
        listening=$(lsof -i :$port -sTCP:LISTEN -P -n | grep LISTEN)
        if [ -n "$listening" ]; then
            process_command=$(echo "$listening" | awk '{print $2}')
            iotdb_check=$(ps -p "$process_command" -o args= | grep "iotdb")
            if [ -n "$iotdb_check" ]; then
              iotdb_listening_ports+=("$port ")
            fi
        fi
      elif command -v netstat >/dev/null 2>&1; then
          listening=$(netstat -tln | awk '{print $4}' | grep ":$port$")
          if [ -n "$listening" ]; then
            process_command=$(echo "$listening" | awk '{print $2}')
            iotdb_check=$(ps -p "$process_command" -o args= | grep "iotdb")
            if [ -n "$iotdb_check" ]; then
              iotdb_listening_ports+=("$port ")
            fi
          fi
      else
          echo "Error: Unable to detect port occupation. Please install 'lsof' or 'netstat' command."
          exit 1
      fi
    done
    if [ ${#iotdb_listening_ports[@]} -gt 0 ]; then
          echo " Please stop IoTDB"  >&2
          exit 1
    fi
}

check_running_process

for f in ${IOTDB_HOME}/lib/*.jar; do
    CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=org.apache.iotdb.tool.backup.IoTDBDataBackTool

logs_dir="${IOTDB_HOME}/logs"

if [ ! -d "$logs_dir" ]; then
    mkdir "$logs_dir"
fi

IOTDB_CLI_CONF=${IOTDB_HOME}/conf
iotdb_cli_params="-Dlogback.configurationFile=${IOTDB_CLI_CONF}/logback-backup.xml"
exec nohup "$JAVA" -DIOTDB_HOME=${IOTDB_HOME} $iotdb_cli_params -cp "$CLASSPATH" "$MAIN_CLASS" "$@" >/dev/null 2>&1 <&- &
