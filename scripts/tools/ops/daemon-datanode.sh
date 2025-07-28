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
IOTDB_SBIN_HOME="$(cd "`dirname "$0"`"/../../sbin; pwd)"
SYSTEMD_DIR="/etc/systemd/system"

if [ ! -d "$SYSTEMD_DIR" ]; then
    echo "Current system can't support systemd"
    exit 1  # Exit with an error status
fi

if [ -z "$JAVA_HOME" ]; then
    echo "JAVA_HOME is not set. Please set the JAVA_HOME environment variable."
    exit 1
fi

FILE_NAME=$SYSTEMD_DIR/iotdb-datanode.service

cat > "$FILE_NAME" <<EOF
[Unit]
Description=iotdb-datanode
Documentation=https://iotdb.apache.org/
After=network.target

[Service]
StandardOutput=null
StandardError=null
LimitNOFILE=65536
Type=simple
User=root
Group=root
Environment=JAVA_HOME=$JAVA_HOME
ExecStart=$IOTDB_SBIN_HOME/start-datanode.sh
Restart=on-failure
SuccessExitStatus=143
RestartSec=5
StartLimitInterval=600s
StartLimitBurst=3
RestartPreventExitStatus=SIGKILL

[Install]
WantedBy=multi-user.target
EOF

echo "Daemon service of IoTDB DataNode has been successfully registered."

systemctl daemon-reload
echo
echo "Do you want to execute 'systemctl start iotdb-datanode'? y/n (default y)"
read -r START_SERVICE
if [[ -z "$START_SERVICE" || "$START_SERVICE" =~ ^[Yy]$ ]]; then
    "${IOTDB_SBIN_HOME}"/sbin/stop-datanode.sh >/dev/null 2>&1 &
    systemctl start iotdb-datanode
    echo "Executed successfully."
fi
echo
echo "Do you want to execute 'systemctl enable iotdb-datanode' to start at boot? y/n (default y)"
read -r ADD_STARTUP
if [[ -z "$ADD_STARTUP" || "$ADD_STARTUP" =~ ^[Yy]$ ]]; then
   systemctl enable iotdb-datanode >/dev/null 2>&1
   echo "Executed successfully."
fi