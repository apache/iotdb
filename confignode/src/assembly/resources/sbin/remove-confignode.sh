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

echo ----------------------------
echo Starting to remove IoTDB ConfigNode
echo ----------------------------

source "$(dirname "$0")/iotdb-common.sh"

#get_iotdb_include wil remove -D parameters
VARS=$(get_iotdb_include "$*")
checkAllConfigNodeVariables
eval set -- "$VARS"

PARAMS="-r "$*

initConfigNodeEnv

CLASSPATH=""
for f in ${CONFIGNODE_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done
classname=org.apache.iotdb.confignode.service.ConfigNode

launch_service() {
  class="$1"
    iotdb_parms="-Dlogback.configurationFile=${CONFIGNODE_LOG_CONFIG}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_HOME=${CONFIGNODE_HOME}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_DATA_HOME=${CONFIGNODE_DATA_HOME}"
  	iotdb_parms="$iotdb_parms -DTSFILE_HOME=${CONFIGNODE_HOME}"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_CONF=${CONFIGNODE_CONF}"
  	iotdb_parms="$iotdb_parms -DTSFILE_CONF=${CONFIGNODE_CONF}"
  	iotdb_parms="$iotdb_parms -Dname=iotdb\.ConfigNode"
  	iotdb_parms="$iotdb_parms -DCONFIGNODE_LOGS=${CONFIGNODE_LOGS}"

  exec "$JAVA" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$class" $PARAMS
  return $?
}

# Start up the service
launch_service "$classname"

exit $?
