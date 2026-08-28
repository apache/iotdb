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

# Stop IoTDB Edge (the merged ConfigNode + DataNode process).

IOTDB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"

if [ -f "${IOTDB_HOME}/edge.pid" ]; then
    PID=$(cat "${IOTDB_HOME}/edge.pid")
    if kill -0 "$PID" 2>/dev/null; then
        kill "$PID"
        for i in $(seq 1 30); do
            kill -0 "$PID" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "$PID" 2>/dev/null; then
            kill -9 "$PID"
        fi
        echo "IoTDB Edge process $PID stopped."
    else
        echo "IoTDB Edge process $PID is not running."
    fi
    rm -f "${IOTDB_HOME}/edge.pid"
else
    echo "No pid file found, trying to stop by process name."
    pkill -f "org.apache.iotdb.edge.EdgeNode" 2>/dev/null
fi
exit 0
