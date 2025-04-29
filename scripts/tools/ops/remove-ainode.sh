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
    echo "The script will remove an AINode."
    echo "When it is necessary to move an already connected AINode out of the cluster, the corresponding removal script can be executed."
    echo "Usage:"
    echo "Remove the AINode with ainode_id"
    echo "./sbin/remove-ainode.sh -t [ainode_id]"
    echo ""
    echo "Options:"
    echo "  -t = ainode_id"
    echo "  -i = When specifying the Python interpreter please enter the address of the executable file of the Python interpreter in the virtual environment. Currently AINode supports virtual environments such as venv, conda, etc. Inputting the system Python interpreter as the installation location is not supported. In order to ensure that scripts are recognized properly, please use absolute paths whenever possible!"
    exit 0
fi

echo ---------------------------
echo Removing IoTDB AINode
echo ---------------------------

IOTDB_AINODE_HOME="$(cd "`dirname "$0"`"/../..; pwd)"

echo "IOTDB_AINODE_HOME: $IOTDB_AINODE_HOME"
chmod u+x $IOTDB_AINODE_HOME/conf/ainode-env.sh
ain_interpreter_dir=$(sed -n 's/^ain_interpreter_dir=\(.*\)$/\1/p' $IOTDB_AINODE_HOME/conf/ainode-env.sh)
ain_system_dir=$(sed -n 's/^ain_system_dir=\(.*\)$/\1/p' $IOTDB_AINODE_HOME/conf/iotdb-ainode.properties)
bash $IOTDB_AINODE_HOME/conf/ainode-env.sh $*
if [ $? -eq 1 ]; then
    echo "Environment check failed. Exiting..."
    exit 1
fi

# fetch parameters with names
while getopts "i:t:rn" opt; do
  case $opt in
    i) p_ain_interpreter_dir="$OPTARG"
    ;;
    r) p_ain_force_reinstall="$OPTARG"
    ;;
    t) p_ain_remove_target="$OPTARG"
    ;;
    n)
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

# If ain_interpreter_dir in parameters is empty:
if [ -z "$p_ain_interpreter_dir" ]; then
  # If ain_interpreter_dir in ../conf/ainode-env.sh is empty, set default value to ../venv/bin/python3
  if [ -z "$ain_interpreter_dir" ]; then
    ain_interpreter_dir="$IOTDB_AINODE_HOME/venv/bin/python3"
  fi
else
  # If ain_interpreter_dir in parameters is not empty, set ain_interpreter_dir to the value in parameters
  ain_interpreter_dir="$p_ain_interpreter_dir"
fi

# If ain_system_dir is empty, set default value to ../data/ainode/system
if [ -z "$ain_system_dir" ]
then
  ain_system_dir="$IOTDB_AINODE_HOME/data/ainode/system"
fi

echo "Script got parameters: ain_interpreter_dir: $ain_interpreter_dir, ain_system_dir: $ain_system_dir"

# check if ain_interpreter_dir is an absolute path
if [[ "$ain_interpreter_dir" != /* ]]; then
    ain_interpreter_dir="$IOTDB_AINODE_HOME/$ain_interpreter_dir"
fi

# Change the working directory to the parent directory
cd "$IOTDB_AINODE_HOME"
ain_ainode_dir=$(dirname "$ain_interpreter_dir")/ainode


if [ -z "$p_ain_remove_target" ]; then
  echo No target AINode set, use system.properties
  $ain_ainode_dir remove
else
  $ain_ainode_dir remove $p_ain_remove_target
fi

if [ $? -eq 1 ]; then
    echo "Remove AINode failed. Exiting..."
    exit 1
fi

bash $IOTDB_AINODE_HOME/sbin/stop-ainode.sh $*

# Remove system directory
rm -rf $ain_system_dir