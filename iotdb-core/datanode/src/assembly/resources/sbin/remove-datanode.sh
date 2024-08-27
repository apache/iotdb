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

if [ "$#" -eq 1 ] && [ "$1" == "--help" ]; then
    echo "The script will remove a DataNode."
    echo "Before removing a DataNode, ensure that the cluster has at least the number of data/schema replicas DataNodes."
    echo "Usage:"
    echo "Remove the DataNode with datanode_id"
    echo "./sbin/remove-datanode.sh [datanode_id]"
    exit 0
fi

echo ---------------------
echo "Starting to remove a DataNode"
echo ---------------------

source "$(dirname "$0")/iotdb-common.sh"

#get_iotdb_include wil remove -D parameters
VARS=$(get_iotdb_include "$*")
checkAllVariables
eval set -- "$VARS"

PARAMS="-r "$*

#initEnv is in iotdb-common.sh
initEnv

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

classname=org.apache.iotdb.db.service.DataNode

launch_service()
{
	class="$1"
  iotdb_parms="-Dlogback.configurationFile=${IOTDB_LOG_CONFIG}"
	iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_DATA_HOME=${IOTDB_DATA_HOME}"
	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
	iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_CONF}"
	iotdb_parms="$iotdb_parms -Dname=iotdb\.IoTDB"
	iotdb_parms="$iotdb_parms -DIOTDB_LOG_DIR=${IOTDB_LOG_DIR}"

	exec "$JAVA" $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$class" $PARAMS
	return $?
}

# Start up the service
launch_service "$classname"

exit $?


