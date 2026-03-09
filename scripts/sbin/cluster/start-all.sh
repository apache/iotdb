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
    export IOTDB_HOME="`dirname "$0"`/../.."
fi
IOTDB_CLUSTER_PATH="${IOTDB_HOME}"/conf/iotdb-cluster.properties
if [ ! -f ${IOTDB_CLUSTER_PATH} ]; then
  exec ${IOTDB_HOME}/sbin/start-standalone.sh
else
  confignodeStr=$(sed '/^confignode_address_list=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  confignodeIps=(${confignodeStr//,/ })
  datanodeStr=$(sed '/^datanode_address_list=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  datanodeIps=(${datanodeStr//,/ })
  serverPort=$(sed '/^ssh_port=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  confignodePath=$(sed '/^confignode_deploy_path=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  datanodePath=$(sed '/^datanode_deploy_path=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
  account=$(sed '/^ssh_account=/!d;s/.*=//' "${IOTDB_CLUSTER_PATH}")
fi

function validateParam() {
  if [[ -z $1 || -z $2 ||  -z $3 ||  -z $4 ||  -z $5 ||  -z $6 ]]; then
    echo "The iotdb-cluster.properties file only contains default settings. It will start 1C1D."
    exec ${IOTDB_HOME}/sbin/start-standalone.sh
    exit
  fi
}
validateParam $confignodeIps $datanodeIps $confignodePath $datanodePath $account $serverPort

if [ "$IOTDB_SSH_OPTS" = "" ]; then
  IOTDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

size=${#datanodeIps[@]}
for confignodeIP in ${confignodeIps[@]};do
  hasDataNode="false"
  for ((i=0; i<$size; i++))
  do
      if [[ "${datanodeIps[$i]}" == "" ]]; then
        continue
      elif [[ "${datanodeIps[$i]}" == *"$confignodeIP"* ]]; then
          hasDataNode="true"
          unset 'datanodeIps[$i]'
          break
      fi
  done
  if [[ "$hasDataNode" == "true" ]]; then
    echo "The system starts the ConfigNode And DataNode of $confignodeIP"
    ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$confignodeIP "
      nohup bash $confignodePath/sbin/start-confignode.sh >/dev/null 2>&1 &
      sleep 3
      nohup bash $datanodePath/sbin/start-datanode.sh >/dev/null 2>&1 &
      "
  else
    echo "The system starts the ConfigNode of $confignodeIP"
    ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$confignodeIP "nohup bash $confignodePath/sbin/start-confignode.sh >/dev/null 2>&1 &"
  fi
done

for datanodeIP in ${datanodeIps[@]};do
  echo "The system starts the DataNode of $datanodeIP"
  ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$datanodeIP "nohup bash $datanodePath/sbin/start-datanode.sh >/dev/null 2>&1 &"
done

echo "Cluster startup complete ..."
