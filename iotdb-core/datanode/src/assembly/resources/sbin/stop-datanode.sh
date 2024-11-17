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
DATANODE_CONF="`dirname "$0"`/../conf"

if [ -f "${DATANODE_CONF}/iotdb-system.properties" ]; then
    dn_rpc_port=`sed '/^dn_rpc_port=/!d;s/.*=//' ${DATANODE_CONF}/iotdb-system.properties`
else
    dn_rpc_port=`sed '/^dn_rpc_port=/!d;s/.*=//' ${DATANODE_CONF}/iotdb-datanode.properties`
fi

if [ -z "$dn_rpc_port" ]; then
    echo "WARNING: dn_rpc_port not found in the configuration file. Using default value dn_rpc_port=6667"
    dn_rpc_port=6667
fi

check_config_unique "dn_rpc_port" "$dn_rpc_port"

force=""

while true; do
    case "$1" in
        -f)
            force="yes"
            break
        ;;
        "")
            #if we do not use getopt, we then have to process the case that there is no argument.
            #in some systems, when there is no argument, shift command may throw error, so we skip directly
            #all others are args to the program
            PARAMS=$*
            break
        ;;
    esac
done

echo "Check whether the rpc_port is used..., port is" $dn_rpc_port

if  type lsof > /dev/null 2>&1 ; then
  PID=$(lsof -t -i:"${dn_rpc_port}" -sTCP:LISTEN)
elif type netstat > /dev/null 2>&1 ; then
  PID=$(netstat -anp 2>/dev/null | grep ":${dn_rpc_port} " | grep ' LISTEN ' | awk '{print $NF}' | sed "s|/.*||g" )
else
  echo ""
  echo " Error: No necessary tool."
  echo " Please install 'lsof' or 'netstat'."
  exit 1
fi

PID_VERIFY=$(ps ax | grep -i 'DataNode' | grep java | grep -v grep | awk '{print $1}')
if [ -z "$PID" ]; then
  echo "No DataNode to stop"
  if [ "$(id -u)" -ne 0 ]; then
    echo "Maybe you can try to run in sudo mode to detect the process."
  fi
  exit 1
elif [[ "${PID_VERIFY}" =~ ${PID} ]]; then
  if [[ "${force}" == "yes" ]]; then
    kill -9 "$PID"
    echo "Force to stop DataNode, PID:" "$PID"
  else
    kill -s TERM "$PID"
    echo "Stop DataNode, PID:" "$PID"
  fi
else
  echo "No DataNode to stop"
  exit 1
fi

