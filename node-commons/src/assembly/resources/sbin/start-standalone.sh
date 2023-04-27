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

if [ -f "$IOTDB_HOME/sbin/start-confignode.sh" ]; then
  export CONFIGNODE_START_PATH="$IOTDB_HOME/sbin/start-confignode.sh"
else
  echo "Can't find start-confignode.sh"
  exit 0
fi

if [ -f "$IOTDB_HOME/sbin/start-datanode.sh" ]; then
  export DATANODE_START_PATH="$IOTDB_HOME/sbin/start-datanode.sh"
else
  echo "Can't find start-datanode.sh"
  exit 0
fi

nohup bash "$CONFIGNODE_START_PATH" >/dev/null 2>&1 &
sleep 3
nohup bash "$DATANODE_START_PATH" >/dev/null 2>&1 &

echo "Execute start-standalone.sh finished, you can see more details in the logs of confignode and datanode"
