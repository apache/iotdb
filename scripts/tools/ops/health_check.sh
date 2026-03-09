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


DATANODE="iotdb-datanode"
CONFIGNODE="iotdb-confignode"

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="`dirname "$0"`/../.."
fi
ulimit_value=""

system_settings_pre_check(){
  ulimit_value=$(ulimit -n)
}

source "${IOTDB_HOME}/conf/iotdb-common.sh"

HELP="Usage: $0 [-ips <ip1> <port1> <port2>,<ip2> <port3> <port4>] [-o <all/local/remote>]"

while [[ $# -gt 0 ]]; do
    case $1 in
        -ips)
            shift
            ips=$1
            while [[ $# -gt 1 ]] && ! [[ ${2:0:1} == "-" ]]; do
                shift
                ips+="-${1}"
            done
            ;;
        -o)
            shift
            operate=$1
            ;;
        --help)
            echo "${HELP}"
            exit 1
            ;;
        *)
            echo "Unrecognized options:$1"
            echo "${HELP}"
            exit 1
            ;;
    esac
    shift
done

ips="${ips//-/ }"
ip_list=$ips


if [ -z "$ip_list" ] ; then
  if [ "$operate" = all ]; then
    echo "Error: -ips is a required option."
    exit 1
  fi
  if [ "$operate" = remote ]; then
    echo "Error: -ips is a required option."
    exit 1
  fi
  if [ -n "$operate" ]; then
      :
  else
    echo "Error: -ips is a required option."
    exit 1
  fi
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

# Convert comma-separated lists to arrays
IFS=',' read -ra ips <<< "$ip_list"

# Check accessibility for each IP address and port combination
for ip in "${ips[@]}"; do
    IFS=' ' read -ra parts <<< "$ip"
    ip="${parts[0]}"
    ports="${parts[@]:1}"
    for port in $ports; do
      formatted_ips+=" $ip $port,"
    done
done
formatted_ips=${formatted_ips%,}

IFS=',' read -ra ips_ports <<< "$formatted_ips"

unreachable_combinations=()
listening_ports=()
iotdb_listening_ports=()
unlistening_ports=()
ip_port_list=""

remote_ports_check() {
  #Check if the port of the remote server is reachable...

  for ip in "${ips[@]}"; do
      IFS=' ' read -ra parts <<< "$ip"
      host="${parts[0]}"
      ports="${parts[@]:1}"
      unreachable_ports=""
      all_ports=""
      for port in $ports; do
           nc -z -v -w 5 "$host" "$port" >/dev/null 2>&1
           result=$?
           all_ports+="$port "
           if [ $result -eq 0 ]; then
              :
           else
             nc_output=$(nc -zvw3 "$host" "$port" 2>&1)
             if echo "$nc_output" | grep -q "Connection refused"; then
               :
             elif echo "$nc_output" | grep -q "timed out"; then
               unreachable_ports+="$port "
             else
               echo "$nc_output"
               unreachable_ports+="$port "
             fi
           fi
      done
      if [ -n "$unreachable_ports" ]; then
        unreachable_combinations+=("IP: $host, Ports: $unreachable_ports")
      fi
      if [ -n "$ip_port_list" ]; then
        ip_port_list="$ip_port_list,$host:$all_ports"
      else
        ip_port_list="$host:$all_ports"
      fi
  done
  echo ""
  echo "Check: Network(Remote Port Connectivity)"
  echo "Requirement:" "$ip_port_list" "need to be accessible"
  echo "Result: "
  # Print the final conclusion of unreachable IP and port combinations
  if [ ${#unreachable_combinations[@]} -gt 0 ]; then
      echo "The following server ports are inaccessible:"
      printf "%s\n" "${unreachable_combinations[@]}"
  else
      echo "All ports are accessible"
  fi
}

# Function to get port value from properties file
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

local_ports_check() {
  #Check if the local port is available....
  dn_rpc_port=$(get_properties_value $DATANODE "dn_rpc_port" "6667")
  dn_internal_port=$(get_properties_value $DATANODE "dn_internal_port" "10730")
  dn_mpp_data_exchange_port=$(get_properties_value $DATANODE "dn_mpp_data_exchange_port" "10740")
  dn_schema_region_consensus_port=$(get_properties_value $DATANODE "dn_schema_region_consensus_port" "10750")
  dn_data_region_consensus_port=$(get_properties_value $DATANODE "dn_data_region_consensus_port" "10760")

  cn_internal_port=$(get_properties_value $CONFIGNODE "cn_internal_port" "10710")
  cn_consensus_port=$(get_properties_value $CONFIGNODE "cn_consensus_port" "10720")

  local_ports+=("$dn_rpc_port")
  local_ports+=("$dn_internal_port")
  local_ports+=("$dn_mpp_data_exchange_port")
  local_ports+=("$dn_schema_region_consensus_port")
  local_ports+=("$dn_data_region_consensus_port")
  local_ports+=("$cn_internal_port")
  local_ports+=("$cn_consensus_port")

  for port in "${local_ports[@]}"; do
    # Check if lsof command is available
    if command -v lsof >/dev/null 2>&1; then
      listening=$(lsof -i :$port -sTCP:LISTEN -P -n | grep LISTEN)
      if [ -n "$listening" ]; then
          process_command=$(echo "$listening" | awk '{print $2}')
          iotdb_check=$(ps -p "$process_command" -o args= | grep "iotdb")
          if [ -n "$iotdb_check" ]; then
            iotdb_listening_ports+=("$port ")
          else
            listening_ports+=("$port ")
          fi
      else
        unlistening_ports+=("$port ")
      fi
    elif command -v netstat >/dev/null 2>&1; then
        listening=$(netstat -tln | awk '{print $4}' | grep ":$port$")
        if [ -n "$listening" ]; then
          process_command=$(echo "$listening" | awk '{print $2}')
          iotdb_check=$(ps -p "$process_command" -o args= | grep "iotdb")
          if [ -n "$iotdb_check" ]; then
            iotdb_listening_ports+=("$port ")
          else
             listening_ports+=("$port ")
          fi
        else
          unlistening_ports+=("$port ")
        fi
    else
        echo "Error: Unable to detect port occupation. Please install 'lsof' or 'netstat' command."
        exit 1
    fi
  done
  echo "Check: Network(Local Port)"
  echo "Requirement: Port" "${local_ports[@]}" "is not occupied"
  echo "Result: "
  # Print the occupied ports
  if [ ${#listening_ports[@]} -gt 0 ]; then
      echo "Port" "${listening_ports[@]}" "occupied by other programs"
  fi

  if [ ${#iotdb_listening_ports[@]} -gt 0 ]; then
      echo "Port" "${iotdb_listening_ports[@]}" "is occupied by IoTDB"
  fi

  if [ ${#unlistening_ports[@]} -gt 0 ]; then
      echo "Port" "${unlistening_ports[@]}" "is free"
  fi
}

local_dirs_check() {
  # Check directory operation permissions...
  dn_data_dirs=$(get_properties_value "iotdb-datanode" "dn_data_dirs" "data/datanode/data")
  dn_consensus_dir=$(get_properties_value "iotdb-datanode" "dn_consensus_dir" "data/datanode/consensus")
  dn_system_dir=$(get_properties_value "iotdb-datanode" "dn_consensus_dir" "data/datanode/system")
  dn_wal_dirs=$(get_properties_value "iotdb-datanode" "dn_wal_dirs" "data/datanode/wal")

  cn_system_dir=$(get_properties_value "iotdb-confignode" "cn_system_dir" "data/confignode/system")
  cn_consensus_dir=$(get_properties_value "iotdb-confignode" "cn_consensus_dir" "data/confignode/consensus")

  pipe_lib_dir=$(get_properties_value "iotdb-common" "pipe_lib_dir" "ext/pipe")
  udf_lib_dir=$(get_properties_value "iotdb-common" "udf_lib_dir" "ext/udf")
  trigger_lib_dir=$(get_properties_value "iotdb-common" "trigger_lib_dir" "ext/trigger")

  node_dirs_list=("$dn_data_dirs","$dn_consensus_dir","$dn_system_dir","$dn_wal_dirs","$cn_system_dir","$cn_consensus_dir","$pipe_lib_dir","$udf_lib_dir","$trigger_lib_dir")

  # Check if the directories exist and are writable or readable
  for node_dirs in "${node_dirs_list[@]}"; do
    IFS=';' read -ra directory <<< "$node_dirs"
    for subdirectory in "${directory[@]}"; do
      IFS=',' read -ra directory <<< "$subdirectory"
      for directory in "${directory[@]}"; do
      if [[ "$directory" == /* ]]; then
        full_directory="$directory"
      else
        full_directory="$IOTDB_HOME/$directory"
      fi
      if [ -d "$full_directory" ]; then
        if [ -w "$full_directory" ] && [ -r "$full_directory" ]; then
          operate_dirs+="$directory has write permission,"
        else
          operate_dirs+="$directory lacks write permission,"
        fi
      else
        parent_directory="$full_directory"
        while [ ! -w "$parent_directory" ] && [ "$parent_directory" != "/" ]; do
          parent_directory=$(dirname "$parent_directory")
          if [ -d "$parent_directory" ]; then
            break
          fi
        done

        if [ ! -w "$parent_directory" ] || [ ! -r "$parent_directory" ]; then
          operate_dirs+="$directory lacks write permission,"
        else
          operate_dirs+="$directory has write permission,"
        fi
      fi
    done
    done
  done
  echo ""
  echo "Check: Installation Environment(Directory Access)"
  echo "Requirement: IoTDB needs" "${node_dirs_list[@]}" "write permission."

  if [ -n "$operate_dirs" ]; then
    echo "Result: "
    echo "$operate_dirs" | tr ',' '\n'
  else
    echo "Result: Directory not found"
  fi
}

local_jdk_check() {
  echo "Check: Installation Environment(JDK)"
  echo "Requirement: JDK Version >=1.8"

  if [ -z $JAVA ] ; then
    echo "Result: Unable to find java executable. Check JAVA_HOME and PATH environment variables. "
  else
    # JDK Version
    jdk_version=$("$JAVA" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo "Result: JDK Version $jdk_version"
  fi
}

local_mem_check() {
  echo ""
  echo "Check: Installation Environment(Memory)"
  echo "Requirement: Allocate sufficient memory for IoTDB"
  # Mem info
  source "${IOTDB_HOME}/conf/confignode-env.sh" >/dev/null 2>&1

  total_memory=$(free -h | awk '/Mem:/ {print $2}')
  confignode_memory=$(echo "scale=2; $memory_size_in_mb / 1024" | bc)
  confignode_memory_formatted=$(printf "%.2f" "$confignode_memory")
  source "${IOTDB_HOME}/conf/datanode-env.sh" >/dev/null 2>&1
  datanode_memory=$(echo "scale=2; $memory_size_in_mb / 1024" | bc)
  datanode_memory_formatted=$(printf "%.2f" "$datanode_memory")
  if [ -n "$confignode_memory" ] && [ -n "$datanode_memory" ]; then
    echo "Result: Total Memory ${total_memory}, ${confignode_memory_formatted} G allocated to IoTDB ConfigNode, ${datanode_memory_formatted} G allocated to IoTDB DataNode"
  elif [ -n "$confignode_memory" ]; then
    echo "Result: Total Memory ${total_memory}, ${confignode_memory_formatted} G allocated to IoTDB ConfigNode"
  elif [ -n "$datanode_memory" ]; then
    echo "Result: Total Memory ${total_memory}, ${datanode_memory_formatted} G allocated to IoTDB DataNode"
  else
    echo "Result: Total Memory ${total_memory}."
  fi
}

system_settings_check() {
  echo ""
  echo "Check: System Settings(Maximum Open Files Number)"
  echo "Requirement: >= 65535"

  # Get the maximum open file limit for the current user

  echo "Result: $ulimit_value"

  echo ""
  echo "Check: System Settings(Swap)"
  echo "Requirement: disabled"
  #Check if Swap is enabled
  swap_status=$(swapon --show)
  if [ -n "$swap_status" ]; then
    echo "Result: enabled."
  else
    echo "Result: disabled."
  fi
}


if [ -n "$operate" ]; then
    if [ "$operate" == "local" ]; then
        system_settings_pre_check
        local_jdk_check
        local_mem_check
        local_dirs_check
        local_ports_check
        system_settings_check
    elif [ "$operate" == "remote" ]; then
        remote_ports_check
    elif  [ "$operate" == "all" ]; then
        system_settings_pre_check
        local_jdk_check
        local_mem_check
        local_dirs_check
        local_ports_check
        remote_ports_check
        system_settings_check
    else
        echo "The parameter - o only has the value local/remote/all (default)"
        exit 1
    fi
else
    system_settings_pre_check
    local_jdk_check
    local_mem_check
    local_dirs_check
    local_ports_check
    remote_ports_check
    system_settings_check
fi
