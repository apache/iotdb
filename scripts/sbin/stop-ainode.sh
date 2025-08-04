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

AINODE_CONF="`dirname "$0"`/../conf"
if [ -f "${AINODE_CONF}/iotdb-ainode.properties" ]; then
    ain_rpc_port=$(sed '/^ain_rpc_port=/!d;s/.*=//' "${AINODE_CONF}"/iotdb-ainode.properties)
    # trim the port
    ain_rpc_port=$(echo "$ain_rpc_port" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
fi

if [ -z "$ain_rpc_port" ]; then
    echo "WARNING: ain_rpc_port not found in the configuration file. Using default value ain_rpc_port=10810"
    ain_rpc_port=10810
fi

# fetch parameters with names
while getopts "i:t:r" opt; do
  case $opt in
    i)
    ;;
    r)
    ;;
    t) p_ain_remove_target="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

# If p_ain_remove_target exists, take the value after the colon of p_ain_remove_target as ain_rpc_port
if [ -n "$p_ain_remove_target" ]; then
  ain_rpc_port=${p_ain_remove_target#*:}
fi

echo "Check whether the rpc_port is used..., port is" $ain_rpc_port

if  type lsof > /dev/null 2>&1 ; then
  echo $(lsof -t -i:"${ain_rpc_port}" -sTCP:LISTEN)
  PID=$(lsof -t -i:"${ain_rpc_port}" -sTCP:LISTEN)
elif type netstat > /dev/null 2>&1 ; then
  PID=$(netstat -anp 2>/dev/null | grep ":${ain_rpc_port} " | grep ' LISTEN ' | awk '{print $NF}' | sed "s|/.*||g" )
else
  echo ""
  echo " Error: No necessary tool."
  echo " Please install 'lsof' or 'netstat'."
  exit 1
fi

PID_VERIFY=$(ps ax | grep -i 'ainode' | grep -v grep | awk '{print $1}')

if [ -z "$PID" ]; then
  echo "No AINode to stop"
  if [ "$(id -u)" -ne 0 ]; then
    echo "Maybe you can try to run in sudo mode to detect the process."
  fi
  exit 1
elif [[ "${PID_VERIFY}" =~ ${PID} ]]; then
  kill -s TERM "$PID"
  echo "Stop AINode, PID:" "$PID"
else
  echo "No AINode to stop"
  exit 1
fi

