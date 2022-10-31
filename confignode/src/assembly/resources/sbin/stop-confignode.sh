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

CONFIGNODE_CONF="$(dirname "$0")/../conf"
internal_port=$(sed '/^internal_port=/!d;s/.*=//' ${CONFIGNODE_CONF}/iotdb-confignode.properties)

echo "check whether the internal_port is used..., port is" $internal_port

if type lsof >/dev/null 2>&1; then
  PID=$(lsof -t -i:${internal_port} -sTCP:LISTEN)
elif type netstat >/dev/null 2>&1; then
  PID=$(netstat -anp 2>/dev/null | grep ":${internal_port} " | grep ' LISTEN ' | awk '{print $NF}' | sed "s|/.*||g")
else
  echo ""
  echo " Error: No necessary tool."
  echo " Please install 'lsof' or 'netstat'."
  exit 1
fi

if [ -z "$PID" ]; then
  echo "No ConfigNode to stop"
  exit 1
else
  kill -s TERM $PID
  echo "close ConfigNode, PID:" $PID
fi
