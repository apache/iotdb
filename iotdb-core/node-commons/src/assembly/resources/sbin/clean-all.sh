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

read -p "Do you want to clean all the data in the IoTDB ? y/n (default n): " CLEAN_SERVICE
if [[ "$CLEAN_SERVICE" != "y" && "$CLEAN_SERVICE" != "Y" ]]; then
  echo "Exiting..."
  exit 0
fi

export IOTDB_HOME="`dirname "$0"`/.."
IOTDB_CLUSTER_PATH="${IOTDB_HOME}"/conf/iotdb-cluster.properties
# iotdb-cluster.properties does not exist, the current ICID is cleaned
if [ ! -f ${IOTDB_CLUSTER_PATH} ]; then
  exec ${IOTDB_HOME}/sbin/clean-datanode.sh -f > /dev/null 2>&1 &
  exec ${IOTDB_HOME}/sbin/clean-confignode.sh -f> /dev/null 2>&1 &
  exec rm -rf ${IOTDB_HOME}/data/
else
  confignodeStr=$(sed '/^confignode_address_list=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  confignodeIps=(${confignodeStr//,/ })
  datanodeStr=$(sed '/^datanode_address_list=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  datanodeIps=(${datanodeStr//,/ })
  serverPort=$(sed '/^ssh_port=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  confignodePath=$(sed '/^confignode_deploy_path=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  datanodePath=$(sed '/^datanode_deploy_path=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  account=$(sed '/^ssh_account=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
#  echo $confignodeIps $datanodeIps $confignodePaths $datanodePaths $account $serverPort
fi

function validateParam() {
  if [[ -z $1 || -z $2 ||  -z $3 ||  -z $4 ||  -z $5 ||  -z $6 ]]; then
    echo "The iotdb-cluster.properties file is incomplete, the current 1C1D will be cleaned ... "
    exec ${IOTDB_HOME}/sbin/clean-datanode.sh -f > /dev/null 2>&1 &
    exec ${IOTDB_HOME}/sbin/clean-confignode.sh -f> /dev/null 2>&1 &
    exit
  fi
}

validateParam $confignodeIps $datanodeIps $confignodePath $datanodePath $account $serverPort

# By default disable strict host key checking
if [ "$IOTDB_SSH_OPTS" = "" ]; then
  IOTDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

# duplicate removal
unique_array=($(awk -v RS=' ' '!a[$1]++' <<< ${datanodeIps[@]}))
  # Clean the DataNode service
for datanodeIP in ${unique_array[@]};do
  hasConfigNode="false"
  for ((i=0; i<${#confignodeIps[@]}; i++))
      do
          # 检查元素是否包含指定字符
          if [[ "${confignodeIps[$i]}" == *"$datanodeIP"* ]]; then
              # 打印包含指定字符的元素
              hasConfigNode="true"
              # 从数组中删除这个元素
              unset 'confignodeIps[$i]'
          fi
      done
      if [[ "$hasConfigNode" == "true" ]]; then
        echo "The system starts to clean data of DataNodes and ConfigNode of $datanodeIP"
        ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$datanodeIP "
          nohup bash $datanodePath/sbin/clean-datanode.sh -f >/dev/null 2>&1 &
          sleep 3
          nohup bash $confignodePath/sbin/clean-confignode.sh -f >/dev/null 2>&1 &
          "
      else
        echo "The system starts to clean data of DataNodes of $datanodeIP"
        ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$datanodeIP "
            nohup bash $datanodePath/sbin/clean-datanode.sh -f >/dev/null 2>&1 & >/dev/null 2>&1 &
          "
      fi
done

# duplicate removal
unique_array=($(awk -v RS=' ' '!a[$1]++' <<< ${confignodeIps[@]}))
  # Clean the ConfigNode service
for confignodeIP in ${unique_array[@]};do
  # confignodeCleanShell=$confignodePath/sbin/clean-confignode.sh
  echo "The system starts to clear data of ConfigNodes of $confignodeIP"
  ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$confignodeIP "
      nohup bash $confignodePath/sbin/clean-confignode.sh -f >/dev/null 2>&1 &
  "
done

echo "Cluster cleanup complete ..."
