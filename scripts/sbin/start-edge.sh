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

# Start IoTDB Edge: ConfigNode + DataNode in one JVM process.

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
fi
if [ -z "${IOTDB_CONF}" ]; then
    export IOTDB_CONF=${IOTDB_HOME}/conf
fi
export IOTDB_DATA_HOME=${IOTDB_DATA_HOME:-${IOTDB_HOME}}
export IOTDB_LOG_DIR=${IOTDB_LOG_DIR:-${IOTDB_HOME}/logs}
mkdir -p "${IOTDB_LOG_DIR}"

source "$(dirname "$0")/../conf/iotdb-common.sh"
export CONFIGNODE_HOME=${IOTDB_HOME}
export CONFIGNODE_DATA_HOME=${IOTDB_DATA_HOME}
export CONFIGNODE_CONF=${IOTDB_CONF}
export CONFIGNODE_LOG_DIR=${IOTDB_LOG_DIR}

# Reuse the same configuration-aware port checks as the standard launchers.
checkAllVariables
checkAllConfigNodeVariables
checkConfigNodePortUsages
checkDataNodePortUsages

. "${IOTDB_CONF}/edge-env.sh"

# find java in JAVA_HOME
if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi
if [ -z "$JAVA" ]; then
    echo "Unable to find java executable. Check JAVA_HOME and PATH environment variables." > /dev/stderr
    exit 1
fi

illegal_access_params=""
illegal_access_params="$illegal_access_params --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.lang=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.util=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.nio=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.io=ALL-UNNAMED"
illegal_access_params="$illegal_access_params --add-opens=java.base/java.net=ALL-UNNAMED"

CLASSPATH=""
for f in "${IOTDB_HOME}"/lib/*.jar; do
    CLASSPATH=${CLASSPATH}":"$f
done

iotdb_parms="-Dlogback.configurationFile=${IOTDB_CONF}/logback-edge.xml"
iotdb_parms="$iotdb_parms -DIOTDB_HOME=${IOTDB_HOME}"
# CONFIGNODE_HOME must also point to the installation directory, otherwise the
# ConfigNode part resolves its data directories against the working directory.
iotdb_parms="$iotdb_parms -DCONFIGNODE_HOME=${IOTDB_HOME}"
iotdb_parms="$iotdb_parms -DIOTDB_DATA_HOME=${IOTDB_DATA_HOME}"
iotdb_parms="$iotdb_parms -DTSFILE_HOME=${IOTDB_HOME}"
iotdb_parms="$iotdb_parms -DIOTDB_CONF=${IOTDB_CONF}"
iotdb_parms="$iotdb_parms -DCONFIGNODE_CONF=${IOTDB_CONF}"
iotdb_parms="$iotdb_parms -DTSFILE_CONF=${IOTDB_CONF}"
iotdb_parms="$iotdb_parms -Dname=iotdb.EdgeNode"
iotdb_parms="$iotdb_parms -DIOTDB_LOG_DIR=${IOTDB_LOG_DIR}"
iotdb_parms="$iotdb_parms -DCONFIGNODE_LOG_DIR=${IOTDB_LOG_DIR}"
iotdb_parms="$iotdb_parms -DOFF_HEAP_MEMORY=${OFF_HEAP_MEMORY}"

classname=org.apache.iotdb.edge.EdgeNode

echo "Starting IoTDB Edge (ConfigNode + DataNode in one process)"
nohup "$JAVA" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS -cp "$CLASSPATH" "$classname" -s > "${IOTDB_LOG_DIR}/log_edge_console.log" 2>&1 &
echo $! > "${IOTDB_HOME}/edge.pid"
echo "IoTDB Edge started, pid $(cat "${IOTDB_HOME}/edge.pid"), console log: ${IOTDB_LOG_DIR}/log_edge_console.log"
