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

# This script is sourced by IoTDB Docker entrypoints before starting ConfigNode
# and/or DataNode. It detects the CPU, memory, and disk resources visible inside
# the container, then exports default JVM options based on those resources.
#
# Why this is needed:
# - Host-level commands such as `free`, `nproc`, and `/proc/cpuinfo` may expose
#   host resources instead of Docker/Kubernetes cgroup limits.
# - Compose examples should not hard-code JVM memory by default, otherwise the
#   normal startup script's memory calculation is bypassed.
# - `-XX:ActiveProcessorCount` makes Java APIs such as
#   Runtime.getRuntime().availableProcessors() follow the container CPU limit.
#
# User supplied JVM options are respected. This script only appends a memory or
# CPU JVM option when that option is not already present.

start_what="$1"

# Allow users to turn off Docker resource detection without changing the image.
if [[ "${IOTDB_DOCKER_RESOURCE_DETECTION}" == "false" ]]; then
  echo "IoTDB docker resource detection is disabled."
  return 0 2>/dev/null || exit 0
fi

# Print the content of the first existing file in the given path list.
# It is used for cgroup v1/v2 files whose locations differ between runtimes.
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

# Count CPUs from cpuset syntax, for example:
# - "0-3" means 4 CPUs
# - "0-1,4,6-7" means 5 CPUs
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

# Integer division rounded up. CPU quota may be smaller than one full core, but
# Java expects an integer ActiveProcessorCount, so keep at least one core later.
function ceil_div() {
  local numerator="$1"
  local denominator="$2"
  echo $(((numerator + denominator - 1) / denominator))
}

# Get the CPU cores visible from the host/process namespace as a fallback when
# cgroup quota and cpuset are unavailable.
function get_host_cpu_cores() {
  local cores
  cores=$(nproc 2>/dev/null || grep -c '^processor' /proc/cpuinfo 2>/dev/null || echo 1)
  if [[ ! "${cores}" =~ ^[0-9]+$ || "${cores}" -lt 1 ]]; then
    cores=1
  fi
  echo "${cores}"
}

# Detect container CPU cores from cgroup quota and cpuset.
# The effective CPU count is the minimum valid value among:
# - CPU quota from cgroup v2 cpu.max or cgroup v1 cpu.cfs_*
# - cpuset range, if configured
# - host-visible CPU cores as fallback
function get_container_cpu_cores() {
  local quota period quota_cores cpuset cpuset_cores host_cores cores

  quota_cores=0
  # cgroup v2 exposes quota and period in one file: "<quota> <period>".
  # The quota value "max" means no CPU quota is configured.
  if [[ -f /sys/fs/cgroup/cpu.max ]]; then
    read -r quota period < /sys/fs/cgroup/cpu.max
    if [[ "${quota}" != "max" && "${quota}" =~ ^[0-9]+$ && "${period}" =~ ^[0-9]+$ && "${period}" -gt 0 ]]; then
      quota_cores=$(ceil_div "${quota}" "${period}")
    fi
  # cgroup v1 stores quota and period in separate cpu controller files.
  # Negative quota means unlimited, so it is ignored.
  elif [[ -f /sys/fs/cgroup/cpu/cpu.cfs_quota_us && -f /sys/fs/cgroup/cpu/cpu.cfs_period_us ]]; then
    quota=$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us)
    period=$(cat /sys/fs/cgroup/cpu/cpu.cfs_period_us)
    if [[ "${quota}" =~ ^-?[0-9]+$ && "${period}" =~ ^[0-9]+$ && "${quota}" -gt 0 && "${period}" -gt 0 ]]; then
      quota_cores=$(ceil_div "${quota}" "${period}")
    fi
  fi

  # cpuset can further restrict the CPUs available to this container.
  cpuset=$(read_first_existing_file \
    /sys/fs/cgroup/cpuset.cpus.effective \
    /sys/fs/cgroup/cpuset.cpus \
    /sys/fs/cgroup/cpuset/cpuset.cpus.effective \
    /sys/fs/cgroup/cpuset/cpuset.cpus 2>/dev/null || true)
  cpuset_cores=$(count_cpuset_cpus "${cpuset}")
  host_cores=$(get_host_cpu_cores)

  # Use the smallest valid value to honor both quota and cpuset restrictions.
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

# Get host-visible memory in MB. This is used only when no valid memory cgroup
# limit exists.
function get_host_memory_mb() {
  local mem_kb mem_mb
  mem_kb=$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  mem_mb=$((mem_kb / 1024))
  if [[ "${mem_mb}" -lt 1 ]]; then
    mem_mb=2048
  fi
  echo "${mem_mb}"
}

# Detect container memory limit in MB.
# cgroup v2 uses memory.max and may contain "max"; cgroup v1 uses
# memory.limit_in_bytes. Very large values are treated as unlimited.
function get_container_memory_mb() {
  local value mem_bytes mem_mb host_mb unlimited_threshold

  value=$(read_first_existing_file \
    /sys/fs/cgroup/memory.max \
    /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true)
  host_mb=$(get_host_memory_mb)
  unlimited_threshold=900000000000000000

  # Only use cgroup memory when it is a real limit smaller than host memory.
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

# Convert an overall memory budget into JVM heap and direct memory options.
# The split follows the existing datanode/confignode-env.sh logic:
# - < 4G: 75% heap
# - 4G to 16G: 80% heap
# - 16G to 128G: 87.5% heap
# - >= 128G: leave 16G for off-heap
function calculate_heap_opts() {
  local memory_size_in_mb="$1"
  local on_heap_memory_size_in_mb off_heap_memory_size_in_mb

  # Keep a small lower bound so tiny or malformed container limits do not create
  # zero-sized heap or direct memory settings.
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

# Append a JVM option only when another option with the same semantic prefix is
# not already present. For example, any existing -Xmx value prevents adding a
# second -Xmx.
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

# Add heap and direct memory options to an existing JVM option string without
# overriding user-supplied values.
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

# Configure the JVM options for one IoTDB node type and export the corresponding
# environment variable consumed by the original IoTDB startup scripts.
function configure_node_jvm_opts() {
  local node="$1"
  local memory_size_in_mb="$2"
  local cpu_cores="$3"

  case "${node}" in
    datanode)
      # IOTDB_JMX_OPTS is read by conf/datanode-env.sh.
      IOTDB_JMX_OPTS=$(append_memory_opts_if_absent "${IOTDB_JMX_OPTS}" "${memory_size_in_mb}")
      IOTDB_JMX_OPTS=$(append_if_absent "${IOTDB_JMX_OPTS}" "-XX:ActiveProcessorCount=${cpu_cores}" "-XX:ActiveProcessorCount=")
      export IOTDB_JMX_OPTS
      ;;
    confignode)
      # CONFIGNODE_JMX_OPTS is read by conf/confignode-env.sh.
      CONFIGNODE_JMX_OPTS=$(append_memory_opts_if_absent "${CONFIGNODE_JMX_OPTS}" "${memory_size_in_mb}")
      CONFIGNODE_JMX_OPTS=$(append_if_absent "${CONFIGNODE_JMX_OPTS}" "-XX:ActiveProcessorCount=${cpu_cores}" "-XX:ActiveProcessorCount=")
      export CONFIGNODE_JMX_OPTS
      ;;
  esac
}

# Read the last active value of a property from an IoTDB properties file.
function get_property_value() {
  local key="$1"
  local file="$2"

  if [[ -f "${file}" ]]; then
    sed -n "s|^[[:space:]]*${key}[[:space:]]*=[[:space:]]*||p" "${file}" | tail -n 1
  fi
}

# Resolve an IoTDB path from the properties file to an absolute path.
# IoTDB supports multiple directories separated by semicolons or commas; disk
# probing only needs one representative path here, so take the first entry.
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

# Collect the disk paths that should represent this container's IoTDB storage.
# The entrypoint calls replace-conf-from-env.sh before sourcing this script, so
# environment-driven directory changes have already been written to properties.
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

# Detect disk capacity and available space for IoTDB storage paths.
# The exported values are informational for startup logs and future diagnostics;
# IoTDB's own disk-space protection remains in the Java process.
function detect_disk() {
  local paths path existing_path info total available min_available selected_path

  min_available=""
  for path in $(collect_disk_paths); do
    # Create configured directories when possible so df reports the actual
    # mounted volume used by IoTDB data/log paths.
    mkdir -p "${path}" 2>/dev/null || true
    existing_path="${path}"
    # If mkdir failed, walk up to an existing parent path for df.
    while [[ ! -e "${existing_path}" && "${existing_path}" != "/" ]]; do
      existing_path=$(dirname "${existing_path}")
    done
    info=$(df -Pm "${existing_path}" 2>/dev/null | awk 'NR==2 {print $2" "$4}')
    if [[ -n "${info}" ]]; then
      total="${info%% *}"
      available="${info##* }"
      # Keep the minimum available space among storage paths because it is the
      # most restrictive disk visible to this node.
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

# Main flow:
# 1. Detect container CPU and memory.
# 2. Split memory between node roles according to current IoTDB defaults.
# 3. Export JVM options for the node(s) being started.
# 4. Detect disk information for startup logging.
container_cpu_cores=$(get_container_cpu_cores)
container_memory_mb=$(get_container_memory_mb)
datanode_memory_mb=$((container_memory_mb / 2))
confignode_memory_mb=$((container_memory_mb / 10 * 3))

# Keep ConfigNode defaults aligned with confignode-env.sh, which caps suggested
# memory at 8G.
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
