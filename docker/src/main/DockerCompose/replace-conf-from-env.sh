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

conf_path=${IOTDB_HOME}/conf

function process_single(){
	local key_value="$1"
	local filename=$2
	local key=$(echo $key_value|cut -d = -f1)
	local line=$(grep -ni "${key}=" ${filename})
	#echo "line=$line"
	if [[ -n "${line}" ]]; then
    echo "update $key $filename"
    local line_no=$(echo $line|cut -d : -f1)
		local content=$(echo $line|cut -d : -f2)
		if [[ "${content:0:1}" != "#" ]]; then
      sed -i "${line_no}d" ${filename}
    fi
    sed -i "${line_no} i${key_value}" ${filename}
	fi
}

function replace_configs(){
  for v in $(env); do
    if [[ "${v}" =~ "=" && "${v}" =~ "_" && ! "${v}" =~ "JAVA_" ]]; then
#      echo "###### $v ####"
      for f in ${target_files}; do
          process_single $v ${conf_path}/$f
      done
    fi
  done
}

case "$1" in
  confignode)
    target_files="iotdb-common.properties iotdb-confignode.properties"
    ;;
  datanode)
    target_files="iotdb-common.properties iotdb-datanode.properties"
    ;;
  all)
    target_files="iotdb-common.properties iotdb-confignode.properties iotdb-datanode.properties"
    ;;
esac

replace_configs

