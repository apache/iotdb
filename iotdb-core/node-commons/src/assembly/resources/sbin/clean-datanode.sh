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

reCheck=$1
echo $reCheck
if [[ "$reCheck" != "-f" ]]; then
  read -p "Do you want to clean all the data in the IoTDB ? y/n (default n): " CLEAN_SERVICE
  if [[ "$CLEAN_SERVICE" != "y" && "$CLEAN_SERVICE" != "Y" ]]; then
    echo "Exiting..."
    exit 0
  fi
fi

if [ -z "${IOTDB_HOME}" ]; then
  export IOTDB_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

rm -rf ${IOTDB_HOME}/data/datanode/
IOTDB_DATANODE_CONFIG=${IOTDB_HOME}/conf/iotdb-datanode.properties
dn_system_dir=$(echo $(grep '^dn_system_dir=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/system") | sed 's/.*=//')
echo clean $dn_system_dir
dn_data_dirs=$(echo $(grep '^dn_data_dirs=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/data") | sed 's/.*=//')
echo clean $dn_data_dirs
dn_consensus_dir=$(echo $(grep '^dn_consensus_dir=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/consensus") | sed 's/.*=//')
echo clean $dn_consensus_dir
dn_wal_dirs=$(echo $(grep '^dn_wal_dirs=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/wal") | sed 's/.*=//')
echo clean $dn_wal_dirs
dn_tracing_dir=$(echo $(grep '^dn_tracing_dir=' ${IOTDB_DATANODE_CONFIG} || echo "datanode/tracing") | sed 's/.*=//')
echo clean $dn_tracing_dir
dn_sync_dir=$(echo $(grep '^dn_sync_dir=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/sync") | sed 's/.*=//')
echo clean $dn_sync_dir
pipe_receiver_file_dirs=$(echo $(grep '^pipe_receiver_file_dirs=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/system/pipe/receiver") | sed 's/.*=//')
echo clean $pipe_receiver_file_dirs
sort_tmp_dir=$(echo $(grep '^sort_tmp_dir=' ${IOTDB_DATANODE_CONFIG} || echo "data/datanode/tmp") | sed 's/.*=//')
echo clean $sort_tmp_dir

function clearPath {
    path_name=$1
    if [ -n  "$path_name" ]; then
      path_name="${path_name#"${path_name%%[![:space:]]*}"}"
      # 使用分号和逗号作为分隔符
      IFS=';,' read -r -a paths <<< "$path_name"
      # 打印数组中的元素
      for path_name in "${paths[@]}"
      do
          if [[ $path_name == /* ]]; then
            rm -rf $path_name
          else
            rm -rf ${IOTDB_HOME}/$path_name
          fi
      done
    fi
}
clearPath $dn_system_dir
clearPath $dn_data_dirs
clearPath $dn_consensus_dir
clearPath $dn_wal_dirs
clearPath $dn_tracing_dir
clearPath $dn_sync_dir
clearPath $pipe_receiver_file_dirs
clearPath $sort_tmp_dir

echo "DataNode clean done ..."