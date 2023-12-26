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
    export IOTDB_HOME="`dirname "$0"`/.."
fi
# iotdb-cluster.properties does not exist, the current 1C1D is started
if [ ! -f ${IOTDB_HOME}/conf/iotdb-cluster.properties ]; then
  exec ${IOTDB_HOME}/sbin/start-standalone.sh
else
  confignodeStr=$(sed '/^confignode_address_list=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  confignodeIps=(${confignodeStr//,/ })
  datanodeStr=$(sed '/^datanode_address_list=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  datanodeIps=(${datanodeStr//,/ })
  serverPort=$(sed '/^ssh_port=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  confignodePath=$(sed '/^confignode_deploy_path=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  datanodePath=$(sed '/^datanode_deploy_path=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  account=$(sed '/^ssh_account=/!d;s/.*=//' "${IOTDB_HOME}"/conf/iotdb-cluster.properties)
  echo $confignodeIps $datanodeIps $confignodePath $datanodePath $account $serverPort
fi

function validateParam() {
  if [[ -z $1 || -z $2 ||  -z $3 ||  -z $4 ||  -z $5 ||  -z $6 ]]; then
    echo "The iotdb-cluster.properties file is incomplete, the current 1C1D will be started ... "
    exec ${IOTDB_HOME}/sbin/start-standalone.sh
    exit
  fi
}
validateParam $confignodeIps $datanodeIps $confignodePath $datanodePath $account $serverPort

# By default disable strict host key checking
if [ "$IOTDB_SSH_OPTS" = "" ]; then
  IOTDB_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

# Start the confignode service
for confignodeIP in ${confignodeIps[@]};do
  # confignodeStartShell=$confignodePath/sbin/start-confignode.sh
  echo "The system starts the ConfigNode of $confignodeIP"
  ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$confignodeIP "nohup bash $confignodePath/sbin/start-confignode.sh >/dev/null 2>&1 &"
  sleep 3
done

# Start the datanode service
for datanodeIP in ${datanodeIps[@]};do
  # datanodeStartShell=$datanodePath/sbin/start-datanode.sh
  echo "The system starts the DataNode of $datanodeIP"
  ssh $IOTDB_SSH_OPTS -p $serverPort ${account}@$datanodeIP "nohup bash $datanodePath/sbin/start-datanode.sh >/dev/null 2>&1 &"
  sleep 3
done

echo "Cluster startup complete ..."
