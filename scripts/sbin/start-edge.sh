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

# Under Git Bash/MSYS, environment variables inherited in Windows form (e.g. IOTDB_HOME set to
# "D:\iotdb" by a parent process) break the ":"-joined classpath below: MSYS only rewrites
# POSIX path lists to ";" for the Windows JVM, so the whole classpath would arrive as one
# invalid entry. Normalize such values to POSIX paths via cygpath. On Linux cygpath is absent
# and this is a no-op.
normalize_msys_path() {
    local value="$1"
    if command -v cygpath >/dev/null 2>&1; then
        case "$value" in
            *'\\'*|[A-Za-z]:[/\\]*|[A-Za-z]:) value="$(cygpath -u "$value")" ;;
        esac
    fi
    printf '%s' "$value"
}

if [ -z "${IOTDB_HOME}" ]; then
    export IOTDB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
else
    export IOTDB_HOME="$(normalize_msys_path "${IOTDB_HOME}")"
fi
if [ -z "${IOTDB_CONF}" ]; then
    export IOTDB_CONF=${IOTDB_HOME}/conf
else
    export IOTDB_CONF="$(normalize_msys_path "${IOTDB_CONF}")"
fi
export IOTDB_DATA_HOME="$(normalize_msys_path "${IOTDB_DATA_HOME:-${IOTDB_HOME}}")"
export IOTDB_LOG_DIR="$(normalize_msys_path "${IOTDB_LOG_DIR:-${IOTDB_HOME}/logs}")"
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

# The default file-system provider is loaded while the JVM is still initializing NIO.2. Keep its
# exploded classes ahead of application JARs to avoid recursive JAR loading through File.toPath()
# (same wiring as start-datanode.sh / start-confignode.sh).
DEFAULT_FILE_SYSTEM_PROVIDER_OPT="-Djava.nio.file.spi.DefaultFileSystemProvider=com.timecho.iotdb.commons.file.SecureFileSystemProvider"

CLASSPATH="${IOTDB_HOME}/lib/bootstrap"
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

# The main class can be overridden (same convention as MAIN_CLASS in start-datanode.bat), e.g. by
# the Edge integration test to boot a license-free entry from an extra lib jar.
classname="${EDGE_MAIN_CLASS:-com.timecho.iotdb.edge.EdgeNode}"

echo "Starting @brand.name@ Edge (ConfigNode + DataNode in one process)"
nohup "$JAVA" $illegal_access_params $iotdb_parms $IOTDB_JMX_OPTS $DEFAULT_FILE_SYSTEM_PROVIDER_OPT -cp "$CLASSPATH" "$classname" -s > "${IOTDB_LOG_DIR}/log_edge_console.log" 2>&1 &
echo $! > "${IOTDB_HOME}/edge.pid"
echo "@brand.name@ Edge started, pid $(cat "${IOTDB_HOME}/edge.pid"), console log: ${IOTDB_LOG_DIR}/log_edge_console.log"
