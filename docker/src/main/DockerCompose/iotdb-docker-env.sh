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

start_what="$1"

if [[ "${IOTDB_DOCKER_RESOURCE_DETECTION}" == "false" ]]; then
  echo "IoTDB docker resource detection is disabled."
  return 0 2>/dev/null || exit 0
fi

function read_first_existing_file() {
  local file
  for file in "$@"; do
    if [[ -f "${file}" ]]; then
      cat "${file}"
      return 0
    fi
  done
  return 1
}

function count_cpuset_cpus() {
  local cpuset="$1"
  local count=0
  local part start end

  cpuset="${cpuset//[[:space:]]/}"
  if [[ -z "${cpuset}" ]]; then
    echo 0
    return
  fi

  IFS=',' read -ra parts <<< "${cpuset}"
  for part in "${parts[@]}"; do
    if [[ "${part}" == *-* ]]; then
      start="${part%-*}"
      end="${part#*-}"
      if [[ "${start}" =~ ^[0-9]+$ && "${end}" =~ ^[0-9]+$ && "${end}" -ge "${start}" ]]; then
        count=$((count + end - start + 1))
      fi
    elif [[ "${part}" =~ ^[0-9]+$ ]]; then
      count=$((count + 1))
    fi
  done
  echo "${count}"
}

function ceil_div() {
  local numerator="$1"
  local denominator="$2"
  echo $(((numerator + denominator - 1) / denominator))
}

function get_host_cpu_cores() {
  local cores
  cores=$(nproc 2>/dev/null || grep -c '^processor' /proc/cpuinfo 2>/dev/null || echo 1)
  if [[ ! "${cores}" =~ ^[0-9]+$ || "${cores}" -lt 1 ]]; then
    cores=1
  fi
  echo "${cores}"
}

function get_container_cpu_cores() {
  local quota period quota_cores cpuset cpuset_cores host_cores cores

  quota_cores=0
  if [[ -f /sys/fs/cgroup/cpu.max ]]; then
    read -r quota period < /sys/fs/cgroup/cpu.max
    if [[ "${quota}" != "max" && "${quota}" =~ ^[0-9]+$ && "${period}" =~ ^[0-9]+$ && "${period}" -gt 0 ]]; then
      quota_cores=$(ceil_div "${quota}" "${period}")
    fi
  elif [[ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
    quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
    period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
    if [[ "${quota}" =~ ^-?[0-9]+$ && "${period}" =~ ^[0-9]+$ && "${quota}" -gt 0 && "${period}" -gt 0 ]]; then
      quota_cores=$(ceil_div "${quota}" "${period}")
    fi
  fi

  cpuset=$(read_first_existing_file \
    /sys/fs/cgroup/cpuset.cpus.effective \
    /sys/fs/cgroup/cpuset.cpus \
    /sys/fs/cgroup/cpuset/cpuset.cpus.effective \
    /sys/fs/cgroup/cpuset/cpuset.cpus 2>/dev/null || true)
  cpuset_cores=$(count_cpuset_cpus "${cpuset}")
  host_cores=$(get_host_cpu_cores)

  cores="${host_cores}"
  if [[ "${quota_cores}" -gt 0 && "${quota_cores}" -lt "${cores}" ]]; then
    cores="${quota_cores}"
  fi
  if [[ "${cpuset_cores}" -gt 0 && "${cpuset_cores}" -lt "${cores}" ]]; then
    cores="${cpuset_cores}"
  fi
  if [[ "${cores}" -lt 1 ]]; then
    cores=1
  fi
  echo "${cores}"
}

function get_host_memory_mb() {
  local mem_kb mem_mb
  mem_kb=$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  mem_mb=$((mem_kb / 1024))
  if [[ "${mem_mb}" -lt 1 ]]; then
    mem_mb=2048
  fi
  echo "${mem_mb}"
}

function get_container_memory_mb() {
  local value mem_bytes mem_mb host_mb unlimited_threshold

  value=$(read_first_existing_file \
    /sys/fs/cgroup/memory.max \
    /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true)
  host_mb=$(get_host_memory_mb)
  unlimited_threshold=900000000000000000

  if [[ "${value}" =~ ^[0-9]+$ && "${value}" -gt 0 && "${value}" -lt "${unlimited_threshold}" ]]; then
    mem_bytes="${value}"
    mem_mb=$((mem_bytes / 1024 / 1024))
    if [[ "${mem_mb}" -gt 0 && "${mem_mb}" -lt "${host_mb}" ]]; then
      echo "${mem_mb}"
      return
    fi
  fi

  echo "${host_mb}"
}

function calculate_heap_opts() {
  local memory_size_in_mb="$1"
  local on_heap_memory_size_in_mb off_heap_memory_size_in_mb

  if [[ "${memory_size_in_mb}" -lt 256 ]]; then
    memory_size_in_mb=256
  fi

  if [[ "${memory_size_in_mb}" -lt 4096 ]]; then
    on_heap_memory_size_in_mb=$((memory_size_in_mb / 4 * 3))
  elif [[ "${memory_size_in_mb}" -lt 16384 ]]; then
    on_heap_memory_size_in_mb=$((memory_size_in_mb / 5 * 4))
  elif [[ "${memory_size_in_mb}" -lt 131072 ]]; then
    on_heap_memory_size_in_mb=$((memory_size_in_mb / 8 * 7))
  else
    on_heap_memory_size_in_mb=$((memory_size_in_mb - 16384))
  fi
  off_heap_memory_size_in_mb=$((memory_size_in_mb - on_heap_memory_size_in_mb))

  echo "-Xms${on_heap_memory_size_in_mb}M -Xmx${on_heap_memory_size_in_mb}M -XX:MaxDirectMemorySize=${off_heap_memory_size_in_mb}M"
}

function append_if_absent() {
  local current_opts="$1"
  local option="$2"
  local match="$3"

  if [[ "${current_opts}" == *"${match}"* ]]; then
    echo "${current_opts}"
  else
    echo "${current_opts} ${option}"
  fi
}

function append_memory_opts_if_absent() {
  local current_opts="$1"
  local memory_size_in_mb="$2"
  local memory_opts item

  memory_opts=$(calculate_heap_opts "${memory_size_in_mb}")
  for item in ${memory_opts}; do
    case "${item}" in
      -Xms*) current_opts=$(append_if_absent "${current_opts}" "${item}" "-Xms") ;;
      -Xmx*) current_opts=$(append_if_absent "${current_opts}" "${item}" "-Xmx") ;;
      -XX:MaxDirectMemorySize=*)
        current_opts=$(append_if_absent "${current_opts}" "${item}" "-XX:MaxDirectMemorySize=")
        ;;
    esac
  done
  echo "${current_opts}"
}

function configure_node_jvm_opts() {
  local node="$1"
  local memory_size_in_mb="$2"
  local cpu_cores="$3"

  case "${node}" in
    datanode)
      IOTDB_JMX_OPTS=$(append_memory_opts_if_absent "${IOTDB_JMX_OPTS}" "${memory_size_in_mb}")
      IOTDB_JMX_OPTS=$(append_if_absent "${IOTDB_JMX_OPTS}" "-XX:ActiveProcessorCount=${cpu_cores}" "-XX:ActiveProcessorCount=")
      export IOTDB_JMX_OPTS
      ;;
    confignode)
      CONFIGNODE_JMX_OPTS=$(append_memory_opts_if_absent "${CONFIGNODE_JMX_OPTS}" "${memory_size_in_mb}")
      CONFIGNODE_JMX_OPTS=$(append_if_absent "${CONFIGNODE_JMX_OPTS}" "-XX:ActiveProcessorCount=${cpu_cores}" "-XX:ActiveProcessorCount=")
      export CONFIGNODE_JMX_OPTS
      ;;
  esac
}

function get_property_value() {
  local key="$1"
  local file="$2"

  if [[ -f "${file}" ]]; then
    sed -n "s|^[[:space:]]*${key}[[:space:]]*=[[:space:]]*||p" "${file}" | tail -n 1
  fi
}

function resolve_iotdb_path() {
  local path="$1"
  local home="$2"

  path="${path%%;*}"
  path="${path%%,*}"
  path="$(echo "${path}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [[ -z "${path}" ]]; then
    return
  fi
  if [[ "${path}" == /* ]]; then
    echo "${path}"
  else
    echo "${home}/${path}"
  fi
}

function collect_disk_paths() {
  local config_file="${IOTDB_HOME}/conf/iotdb-system.properties"
  local paths value

  if [[ "${start_what}" == "datanode" || "${start_what}" == "all" ]]; then
    value=$(get_property_value "dn_data_dirs" "${config_file}")
    paths="${paths} $(resolve_iotdb_path "${value:-data/datanode/data}" "${IOTDB_HOME}")"
    value=$(get_property_value "dn_wal_dirs" "${config_file}")
    paths="${paths} $(resolve_iotdb_path "${value:-data/datanode/wal}" "${IOTDB_HOME}")"
    value=$(get_property_value "dn_system_dir" "${config_file}")
    paths="${paths} $(resolve_iotdb_path "${value:-data/datanode/system}" "${IOTDB_HOME}")"
  fi

  if [[ "${start_what}" == "confignode" || "${start_what}" == "all" ]]; then
    value=$(get_property_value "cn_system_dir" "${config_file}")
    paths="${paths} $(resolve_iotdb_path "${value:-data/confignode/system}" "${IOTDB_HOME}")"
    value=$(get_property_value "cn_consensus_dir" "${config_file}")
    paths="${paths} $(resolve_iotdb_path "${value:-data/confignode/consensus}" "${IOTDB_HOME}")"
  fi

  echo "${paths}"
}

function detect_disk() {
  local paths path existing_path info total available min_available selected_path

  min_available=""
  for path in $(collect_disk_paths); do
    mkdir -p "${path}" 2>/dev/null || true
    existing_path="${path}"
    while [[ ! -e "${existing_path}" && "${existing_path}" != "/" ]]; do
      existing_path=$(dirname "${existing_path}")
    done
    info=$(df -Pm "${existing_path}" 2>/dev/null | awk 'NR==2 {print $2" "$4}')
    if [[ -n "${info}" ]]; then
      total="${info%% *}"
      available="${info##* }"
      if [[ -z "${min_available}" || "${available}" -lt "${min_available}" ]]; then
        min_available="${available}"
        IOTDB_DOCKER_DISK_TOTAL_MB="${total}"
        IOTDB_DOCKER_DISK_AVAILABLE_MB="${available}"
        selected_path="${path}"
      fi
    fi
  done

  if [[ -n "${min_available}" ]]; then
    IOTDB_DOCKER_DISK_PATH="${selected_path}"
    export IOTDB_DOCKER_DISK_PATH IOTDB_DOCKER_DISK_TOTAL_MB IOTDB_DOCKER_DISK_AVAILABLE_MB
  fi
}

container_cpu_cores=$(get_container_cpu_cores)
container_memory_mb=$(get_container_memory_mb)
datanode_memory_mb=$((container_memory_mb / 2))
confignode_memory_mb=$((container_memory_mb / 10 * 3))

if [[ "${confignode_memory_mb}" -gt 8192 ]]; then
  confignode_memory_mb=8192
fi
if [[ "${datanode_memory_mb}" -lt 256 ]]; then
  datanode_memory_mb=256
fi
if [[ "${confignode_memory_mb}" -lt 256 ]]; then
  confignode_memory_mb=256
fi

case "${start_what}" in
  datanode)
    configure_node_jvm_opts datanode "${datanode_memory_mb}" "${container_cpu_cores}"
    ;;
  confignode)
    configure_node_jvm_opts confignode "${confignode_memory_mb}" "${container_cpu_cores}"
    ;;
  all)
    configure_node_jvm_opts confignode "${confignode_memory_mb}" "${container_cpu_cores}"
    configure_node_jvm_opts datanode "${datanode_memory_mb}" "${container_cpu_cores}"
    ;;
esac

detect_disk

export IOTDB_DOCKER_CPU_CORES="${container_cpu_cores}"
export IOTDB_DOCKER_MEMORY_MB="${container_memory_mb}"

echo "IoTDB docker detected CPU cores: ${IOTDB_DOCKER_CPU_CORES}, memory: ${IOTDB_DOCKER_MEMORY_MB}M"
if [[ -n "${IOTDB_DOCKER_DISK_AVAILABLE_MB}" ]]; then
  echo "IoTDB docker detected disk path: ${IOTDB_DOCKER_DISK_PATH}, total: ${IOTDB_DOCKER_DISK_TOTAL_MB}M, available: ${IOTDB_DOCKER_DISK_AVAILABLE_MB}M"
fi
