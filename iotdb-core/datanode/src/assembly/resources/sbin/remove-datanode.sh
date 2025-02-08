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
    echo "The script will remove one or more DataNodes."
    echo "Before removing DataNodes, ensure that the cluster has at least the required number of data/schema replicas DataNodes."
    echo "Usage: ./sbin/remove-datanode.sh [DataNode_ID ...]"
    echo "Remove one or more DataNodes by specifying their IDs."
    echo "Note that this datanode is removed by default if DataNode_ID is not specified."
    echo "Example:"
    echo "  ./sbin/remove-datanode.sh 1         # Remove DataNode with ID 1"
    echo "  ./sbin/remove-datanode.sh 1 2 3     # Remove DataNodes with IDs 1, 2, and 3"
    exit 0
fi

# Check for duplicate DataNode IDs
ids=("$@")
unique_ids=($(printf "%s\n" "${ids[@]}" | sort -u))
if [ "${#ids[@]}" -ne "${#unique_ids[@]}" ]; then
    echo "Error: Duplicate DataNode IDs found."
    exit 1
fi

echo ---------------------
echo "Starting to remove DataNodes: ${ids[*]}"
echo ---------------------

source "$(dirname "$0")/iotdb-common.sh"

#get_iotdb_include will remove -D parameters
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

  # In case the 2g memory is not enough in some scenarios, users can further reduce the memory usage manually.
	# on heap memory size
  ON_HEAP_MEMORY="2G"
  # off heap memory size
  OFF_HEAP_MEMORY="512M"

	exec "$JAVA" $iotdb_parms $illegal_access_params $IOTDB_JMX_OPTS -Xms${ON_HEAP_MEMORY} -Xmx${ON_HEAP_MEMORY} -XX:MaxDirectMemorySize=${OFF_HEAP_MEMORY} -cp "$CLASSPATH" "$class" $PARAMS
	return $?
}

# Start up the service
launch_service "$classname"

exit $?


