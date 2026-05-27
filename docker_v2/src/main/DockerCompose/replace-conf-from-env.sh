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
target_files="iotdb-system.properties"

echo "#################################"
echo "replace the following configs"
echo "conf_path=$conf_path"
echo "target_files=$target_files"

function write_config_value(){
  local key_value="$1"
  local filename="$2"
  local line_no="$3"
  local insert_after="$4"
  local tmp_file

  tmp_file=$(mktemp "${filename}.XXXXXX")
  if ! CONFIG_KEY_VALUE="${key_value}" awk -v target_line="${line_no}" -v insert_after="${insert_after}" '
    target_line > 0 && NR == target_line {
      if (insert_after == "true") {
        print
        print ENVIRON["CONFIG_KEY_VALUE"]
      } else {
        print ENVIRON["CONFIG_KEY_VALUE"]
      }
      next
    }
    { print }
    END {
      if (target_line == 0) {
        print ENVIRON["CONFIG_KEY_VALUE"]
      }
    }
  ' "${filename}" > "${tmp_file}"; then
    rm -f "${tmp_file}"
    return 1
  fi

  if ! mv "${tmp_file}" "${filename}"; then
    rm -f "${tmp_file}"
    return 1
  fi
}

function process_single(){
	local key_value="$1"
	local filename="$2"
	local key="${key_value%%=*}"
	local line line_no content

	if [[ "${key_value}" != *"="* ]]; then
	  return
	fi
	if [[ "${key_value}" == *$'\n'* || "${key_value}" == *$'\r'* ]]; then
	  echo "skip ${key}: multi-line values are not supported in properties files" >&2
	  return
	fi

	line=$(CONFIG_KEY="${key}" awk '
	  index($0, ENVIRON["CONFIG_KEY"] "=") == 1 {
	    print NR ":" $0
	    exit
	  }
	' "${filename}")
	if [[ -z "${line}" ]]; then
	  line=$(CONFIG_KEY="${key}" awk '
	    index($0, "#" ENVIRON["CONFIG_KEY"] "=") == 1 {
	      print NR ":" $0
	      exit
	    }
	  ' "${filename}")
	fi
	#echo "line=$line"
	if [[ -n "${line}" ]]; then
    echo "update $key_value $filename"
    line_no="${line%%:*}"
    content="${line#*:}"
    if [[ "${content:0:1}" != "#" ]]; then
      write_config_value "${key_value}" "${filename}" "${line_no}" "false"
    else
      write_config_value "${key_value}" "${filename}" "${line_no}" "true"
    fi
  else
    echo "append $key_value $filename"
    write_config_value "${key_value}" "${filename}" "0" "false"
	fi
}

function replace_configs(){
  while IFS= read -r -d '' v; do
    key_name="${v%%=*}"
    if [[ "${key_name}" == "${key_name,,}" && ! "${key_name}" =~ ^_ ]]; then
#      echo "###### $v ####"
      for f in ${target_files}; do
          process_single "${v}" "${conf_path}/${f}"
      done
    fi
  done < <(env -0)
}

replace_configs

echo "#################################"
