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

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi

reCheck=$1
if [[ "$reCheck" != "-f" ]]; then
  read -p "Do you want to clean data of confignode in the IoTDB ? y/n (default n): " CLEAN_SERVICE
  if [[ "$CLEAN_SERVICE" != "y" && "$CLEAN_SERVICE" != "Y" ]]; then
    echo "Exiting..."
    exit 1
  fi
fi
nohup bash ${IOTDB_HOME}/sbin/stop-confignode.sh -f >/dev/null 2>&1 &
rm -rf ${IOTDB_HOME}/data/confignode/ >/dev/null 2>&1 &

if [ -f "${IOTDB_HOME}/conf/iotdb-system.properties" ]; then
  IOTDB_CONFIGNODE_CONFIG="${IOTDB_HOME}/conf/iotdb-system.properties"
else
  IOTDB_CONFIGNODE_CONFIG="${IOTDB_HOME}/conf/iotdb-confignode.properties"
fi

cn_system_dir=$(echo $(grep '^cn_system_dir=' ${IOTDB_CONFIGNODE_CONFIG} || echo "data/confignode/system") | sed 's/.*=//')
cn_consensus_dir=$(echo $(grep '^cn_consensus_dir=' ${IOTDB_CONFIGNODE_CONFIG} || echo "data/confignode/consensus") | sed 's/.*=//')

function clearPath {
    path_name=$1
    if [ -n  "$path_name" ]; then
      path_name="${path_name#"${path_name%%[![:space:]]*}"}"
      if [[ $path_name == /* ]]; then
        rm -rf $path_name  >/dev/null 2>&1 &
      else
        rm -rf ${IOTDB_HOME}/$path_name  >/dev/null 2>&1 &
      fi
    fi
}
clearPath $cn_system_dir
clearPath $cn_consensus_dir

echo "ConfigNode clean done ..."