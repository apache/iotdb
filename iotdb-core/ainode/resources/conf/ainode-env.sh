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

# The defaulte venv environment is used if ain_interpreter_dir is not set. Please use absolute path without quotation mark
# ain_interpreter_dir=

# Set ain_force_reinstall to 1 to force reinstall AINode
ain_force_reinstall=0

# don't install dependencies online
ain_install_offline=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# fetch parameters with names
while getopts "i:t:rnm:" opt; do
  case $opt in
  i)
    p_ain_interpreter_dir="$OPTARG"
    ;;
  r)
    p_ain_force_reinstall=1
    ;;
  t) ;;
  n)
    p_ain_no_dependencies="--no-dependencies"
    ;;
  m)
    p_pypi_mirror="$OPTARG"
    ;;
  \?)
    echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done

if [ -z "$p_ain_interpreter_dir" ]; then
  echo "No interpreter_dir is set, use default value."
else
  ain_interpreter_dir="$p_ain_interpreter_dir"
fi

if [ -z "$p_ain_force_reinstall" ]; then
  echo "No check_version is set, use default value."
else
  ain_force_reinstall="$p_ain_force_reinstall"
fi
echo Script got inputs: "ain_interpreter_dir: $ain_interpreter_dir", "ain_force_reinstall: $ain_force_reinstall"

if [ -z $ain_interpreter_dir ]; then
  $(dirname "$0")/../venv/bin/python3 -c "import sys; print(sys.executable)" &&
    echo "Activate default venv environment" || (
    echo "Creating default venv environment" && python3 -m venv "$(dirname "$0")/../venv"
  )
  ain_interpreter_dir="$SCRIPT_DIR/../venv/bin/python3"
fi
echo "Calling venv to check: $ain_interpreter_dir"

# Change the working directory to the parent directory
cd "$SCRIPT_DIR/.."

echo "Confirming AINode..."
$ain_interpreter_dir -m pip config set global.disable-pip-version-check true
$ain_interpreter_dir -m pip list | grep "apache-iotdb-ainode" >/dev/null
if [ $? -eq 0 ]; then
  if [ $ain_force_reinstall -eq 0 ]; then
    echo "AINode is already installed"
    exit 0
  fi
fi

ain_only_ainode=1

# if $ain_install_offline is 1 then do not install dependencies
if [ $ain_install_offline -eq 1 ]; then
  # if offline and not -n, then install dependencies
  if [ -z "$p_ain_no_dependencies" ]; then
    ain_only_ainode=0
  else
    ain_only_ainode=1
  fi
  p_ain_no_dependencies="--no-dependencies"
  echo "Installing AINode offline----without dependencies..."
fi

if [ $ain_force_reinstall -eq 1 ]; then
  p_ain_force_reinstall="--force-reinstall"
else
  p_ain_force_reinstall=""
fi

echo "Installing AINode..."
cd "$SCRIPT_DIR/../lib/"
shopt -s nullglob
for i in *.whl *.tar.gz; do
  if [[ $i =~ "ainode" ]]; then
    echo Installing AINode body: $i
    if [ -z "$p_pypi_mirror" ]; then
      $ain_interpreter_dir -m pip install "$i" $p_ain_force_reinstall --no-warn-script-location $p_ain_no_dependencies --find-links https://download.pytorch.org/whl/cpu/torch_stable.html
    else
      $ain_interpreter_dir -m pip install "$i" $p_ain_force_reinstall -i $p_pypi_mirror --no-warn-script-location $p_ain_no_dependencies --find-links https://download.pytorch.org/whl/cpu/torch_stable.html
    fi
  else
    # if ain_only_ainode is 0 then install dependencies
    if [ $ain_only_ainode -eq 0 ]; then
      echo Installing dependencies $i
      if [ -z "$p_pypi_mirror" ]; then
        $ain_interpreter_dir -m pip install "$i" $p_ain_force_reinstall --no-warn-script-location $p_ain_no_dependencies
      else
        $ain_interpreter_dir -m pip install "$i" $p_ain_force_reinstall -i $p_pypi_mirror --no-warn-script-location $p_ain_no_dependencies
      fi
    fi
  fi
  if [ $? -eq 1 ]; then
    echo "Failed to install AINode"
    exit 1
  fi
done
echo "AINode is installed successfully"
exit 0
