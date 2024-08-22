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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "SCRIPT_DIR: $SCRIPT_DIR"
chmod u+x $(dirname "$0")/../conf/ainode-env.sh
ain_interpreter_dir=$(sed -n 's/^ain_interpreter_dir=\(.*\)$/\1/p' $(dirname "$0")/../conf/ainode-env.sh)
bash $(dirname "$0")/../conf/ainode-env.sh $*
if [ $? -eq 1 ]; then
  echo "Environment check failed. Exiting..."
  exit 1
fi


# fetch parameters with names
while getopts "i:rn" opt; do
  case $opt in
    i) p_ain_interpreter_dir="$OPTARG"
    ;;
    r) p_ain_force_reinstall="$OPTARG"
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
    ain_interpreter_dir="$SCRIPT_DIR/../venv/bin/python3"
  fi
else
  # If ain_interpreter_dir in parameters is not empty, set ain_interpreter_dir to the value in parameters
  ain_interpreter_dir="$p_ain_interpreter_dir"
fi

# check if ain_interpreter_dir is an absolute path
if [[ "$ain_interpreter_dir" != /* ]]; then
  ain_interpreter_dir="$SCRIPT_DIR/$ain_interpreter_dir"
fi

echo Script got parameter: ain_interpreter_dir: $ain_interpreter_dir

# Change the working directory to the parent directory
cd "$SCRIPT_DIR/.."

ain_ainode_dir=$(dirname "$ain_interpreter_dir")/ainode

echo Script got ainode dir: ain_ainode_dir: $ain_ainode_dir

echo Starting AINode...

$ain_ainode_dir start
