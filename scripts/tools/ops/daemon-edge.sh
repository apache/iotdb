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

set -e

IOTDB_HOME="${IOTDB_HOME:-$(cd "$(dirname "$0")"/../.. && pwd)}"
IOTDB_HOME="$(cd "$IOTDB_HOME" && pwd -P)"
export IOTDB_HOME
IOTDB_SBIN_HOME="$IOTDB_HOME/sbin"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"

if [ ! -d "$SYSTEMD_DIR" ] || ! command -v systemctl >/dev/null 2>&1; then
    echo "Current system can't support systemd."
    exit 1
fi

JAVA=java
if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME/bin/java"
    if [ -x "$JAVA_HOME/bin/amd64/java" ]; then
        JAVA="$JAVA_HOME/bin/amd64/java"
    fi
fi
if ! "$JAVA" --version >/dev/null 2>&1; then
    echo "Java is not available. Please check JAVA_HOME and PATH."
    exit 1
fi

if [ ! -x "$IOTDB_SBIN_HOME/start-edge.sh" ] || [ ! -x "$IOTDB_SBIN_HOME/stop-edge.sh" ]; then
    echo "Cannot find the executable Edge start/stop scripts in $IOTDB_SBIN_HOME."
    exit 1
fi

# Quote environment values and executable paths using systemd's syntax.
systemd_quote() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    value="${value//%/%%}"
    printf '"%s"' "$value"
}

FILE_NAME="$SYSTEMD_DIR/iotdb-edge.service"
cat > "$FILE_NAME" <<EOF
[Unit]
Description=iotdb-edge
Documentation=https://iotdb.apache.org/
After=network.target
StartLimitIntervalSec=600s
StartLimitBurst=3

[Service]
StandardOutput=null
StandardError=null
LimitNOFILE=65536
# start-edge.sh forks the JVM and records its PID before returning.
Type=forking
PIDFile=${IOTDB_HOME//%/%%}/edge.pid
WorkingDirectory=${IOTDB_HOME//%/%%}
User=root
Group=root
Environment=$(systemd_quote "JAVA_HOME=$JAVA_HOME")
Environment=$(systemd_quote "PATH=${JAVA_HOME:+$JAVA_HOME/bin:}$PATH")
Environment=$(systemd_quote "IOTDB_HOME=$IOTDB_HOME")
Environment=$(systemd_quote "IOTDB_CONF=${IOTDB_CONF:-$IOTDB_HOME/conf}")
Environment=$(systemd_quote "IOTDB_DATA_HOME=${IOTDB_DATA_HOME:-$IOTDB_HOME}")
Environment=$(systemd_quote "IOTDB_LOG_DIR=${IOTDB_LOG_DIR:-$IOTDB_HOME/logs}")
ExecStart=$(systemd_quote "$IOTDB_SBIN_HOME/start-edge.sh")
ExecStop=$(systemd_quote "$IOTDB_SBIN_HOME/stop-edge.sh")
Restart=on-failure
SuccessExitStatus=143
RestartSec=5
RestartPreventExitStatus=SIGKILL

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
echo "Daemon service of IoTDB Edge has been successfully registered."
echo
echo "Do you want to execute 'systemctl start iotdb-edge'? y/n (default y)"
read -r START_SERVICE || START_SERVICE=""
if [[ -z "$START_SERVICE" || "$START_SERVICE" =~ ^[Yy]$ ]]; then
    # Stop both an existing service and a manually started Edge before starting.
    systemctl stop iotdb-edge
    "$IOTDB_SBIN_HOME/stop-edge.sh"
    systemctl start iotdb-edge
    echo "Executed successfully."
fi
echo
echo "Do you want to execute 'systemctl enable iotdb-edge' to start at boot? y/n (default y)"
read -r ADD_STARTUP || ADD_STARTUP=""
if [[ -z "$ADD_STARTUP" || "$ADD_STARTUP" =~ ^[Yy]$ ]]; then
    systemctl enable iotdb-edge
    echo "Executed successfully."
fi
