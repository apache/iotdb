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

echo ---------------------------
echo Starting IoTDB AINode
echo ---------------------------

IOTDB_AINODE_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export IOTDB_AINODE_HOME
echo "IOTDB_AINODE_HOME: $IOTDB_AINODE_HOME"

# fetch parameters with names
daemon_mode=false
while getopts "i:rnd" opt; do
  case $opt in
    n)
    ;;
    d)
      daemon_mode=true
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

ain_ainode_executable="$IOTDB_AINODE_HOME/lib/ainode"

echo Script got ainode executable: "$ain_ainode_executable"

if [ "$daemon_mode" = true ]; then
  echo Starting AINode in daemon mode...
  nohup $ain_ainode_executable start > /dev/null 2>&1 &
  echo AINode started in background
else
  echo Starting AINode...
  $ain_ainode_executable start
fi
