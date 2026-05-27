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

set -eo pipefail

current_path=$(cd "$(dirname "$0")"; pwd)
start_what="${1:-}"
if [[ -n "${start_what}" ]]; then
  shift
fi

export IOTDB_HOME="${IOTDB_HOME:-/iotdb}"
export IOTDB_DATA_HOME="${IOTDB_DATA_HOME:-${IOTDB_HOME}}"
export IOTDB_CONF="${IOTDB_CONF:-${IOTDB_HOME}/conf}"
export IOTDB_LOG_DIR="${IOTDB_LOG_DIR:-${IOTDB_HOME}/logs}"
export IOTDB_LOG_CONFIG="${IOTDB_LOG_CONFIG:-${IOTDB_CONF}/logback-datanode.xml}"

export CONFIGNODE_HOME="${CONFIGNODE_HOME:-${IOTDB_HOME}}"
export CONFIGNODE_DATA_HOME="${CONFIGNODE_DATA_HOME:-${CONFIGNODE_HOME}}"
export CONFIGNODE_CONF="${CONFIGNODE_CONF:-${CONFIGNODE_HOME}/conf}"
export CONFIGNODE_LOG_DIR="${CONFIGNODE_LOG_DIR:-${CONFIGNODE_HOME}/logs}"
export CONFIGNODE_LOGS="${CONFIGNODE_LOGS:-${CONFIGNODE_LOG_DIR}}"
export CONFIGNODE_LOG_CONFIG="${CONFIGNODE_LOG_CONFIG:-${CONFIGNODE_CONF}/logback-confignode.xml}"

export IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS:-}"
export CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS:-}"
export IOTDB_JVM_OPTS="${IOTDB_JVM_OPTS:-}"

IOTDB_STOP_FLUSH_TIMEOUT="${IOTDB_STOP_FLUSH_TIMEOUT:-3}"
DATANODE_PID=""
CONFIGNODE_PID=""
STOPPING=0

function log() {
  echo "[entrypoint] $*" >&2
}

function usage() {
  echo "Usage: entrypoint.sh {datanode|confignode} [node args]" >&2
}

function findJava() {
  if [[ -n "${JAVA_HOME:-}" ]]; then
    for java in "${JAVA_HOME}"/bin/amd64/java "${JAVA_HOME}"/bin/java; do
      if [[ -x "${java}" ]]; then
        echo "${java}"
        return
      fi
    done
  fi
  echo "java"
}

function buildClasspath() {
  local home="$1"
  local classpath=""
  local jar
  for jar in "${home}"/lib/*.jar; do
    if [[ -e "${jar}" ]]; then
      classpath="${classpath}:${jar}"
    fi
  done
  echo "${classpath}"
}

function addJdkOpensIfNeeded() {
  local java="$1"
  local version_output jvmver major

  version_output=$("${java}" -version 2>&1 || true)
  jvmver=$(echo "${version_output}" | grep -E '^(openjdk|java) version' | awk -F'"' 'NR==1 {print $2}' | cut -d- -f1)
  major="${jvmver%%.*}"
  if [[ "${major}" != "1" ]]; then
    illegal_access_params+=(
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
      "--add-opens=java.base/java.util=ALL-UNNAMED"
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
      "--add-opens=java.base/java.io=ALL-UNNAMED"
      "--add-opens=java.base/java.net=ALL-UNNAMED"
    )
  fi
}

function appendJvmOptIfAbsent() {
  local opts="$1"
  local option="$2"
  local pattern="$3"
  if [[ "${opts}" == *"${pattern}"* ]]; then
    echo "${opts}"
  else
    echo "${opts} ${option}"
  fi
}

function extractJvmOptValue() {
  local opts="$1"
  local prefix="$2"
  local item
  for item in ${opts}; do
    if [[ "${item}" == "${prefix}"* ]]; then
      echo "${item#${prefix}}"
      return 0
    fi
  done
  return 1
}

function parseMemoryMb() {
  local value="$1"
  if [[ "${value}" =~ ^[0-9]+[Gg]$ ]]; then
    echo $((${value%?} * 1024))
  elif [[ "${value}" =~ ^[0-9]+[Mm]$ ]]; then
    echo "${value%?}"
  elif [[ "${value}" =~ ^[0-9]+$ ]]; then
    echo "${value}"
  else
    return 1
  fi
}

# Print the content of the first existing file in the given path list.
# Docker runtimes may expose cgroup v1/v2 memory limits at different paths.
function readFirstExistingFile() {
  local file
  for file in "$@"; do
    if [[ -f "${file}" ]]; then
      cat "${file}"
      return 0
    fi
  done
  return 1
}

# Get host-visible memory in MB. This is only a fallback when the container has
# no explicit memory cgroup limit.
function getHostMemoryMb() {
  local mem_kb mem_mb
  mem_kb=$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  mem_mb=$((mem_kb / 1024))
  if [[ "${mem_mb}" -lt 1 ]]; then
    mem_mb=2048
  fi
  echo "${mem_mb}"
}

# Detect the memory limit visible to this container in MB.
# cgroup v2 uses memory.max and may contain "max"; cgroup v1 uses
# memory.limit_in_bytes. Very large numeric values are treated as unlimited.
function getContainerMemoryMb() {
  local value mem_bytes mem_mb host_mb unlimited_threshold

  value=$(readFirstExistingFile \
    /sys/fs/cgroup/memory.max \
    /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || true)
  host_mb=$(getHostMemoryMb)
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

function normalizeMemoryBudgetMb() {
  local memory_size_in_mb="$1"
  if [[ "${memory_size_in_mb}" -lt 256 ]]; then
    echo 256
  else
    echo "${memory_size_in_mb}"
  fi
}

# Convert a node memory budget into JVM heap and direct memory options.
# The heap/direct split follows the existing IoTDB env script rule:
# - < 4G: 75% heap
# - 4G to 16G: 80% heap
# - 16G to 128G: 87.5% heap
# - >= 128G: reserve 16G for off-heap
function calculateHeapOpts() {
  local memory_size_in_mb="$1"
  local on_heap_memory_size_in_mb off_heap_memory_size_in_mb

  memory_size_in_mb=$(normalizeMemoryBudgetMb "${memory_size_in_mb}")

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

# Add heap and direct memory options to an existing JVM option string without
# overriding user-supplied -Xms, -Xmx, or MaxDirectMemorySize values.
function appendMemoryOptsIfAbsent() {
  local current_opts="$1"
  local memory_size_in_mb="$2"
  local memory_opts item

  memory_opts=$(calculateHeapOpts "${memory_size_in_mb}")
  for item in ${memory_opts}; do
    case "${item}" in
      -Xms*) current_opts=$(appendJvmOptIfAbsent "${current_opts}" "${item}" "-Xms") ;;
      -Xmx*) current_opts=$(appendJvmOptIfAbsent "${current_opts}" "${item}" "-Xmx") ;;
      -XX:MaxDirectMemorySize=*)
        current_opts=$(appendJvmOptIfAbsent "${current_opts}" "${item}" "-XX:MaxDirectMemorySize=")
        ;;
    esac
  done
  echo "${current_opts}"
}

function configureNodeMemoryOpts() {
  local node="$1"
  local memory_size_in_mb
  memory_size_in_mb=$(normalizeMemoryBudgetMb "$2")

  case "${node}" in
    datanode)
      IOTDB_JMX_OPTS=$(appendMemoryOptsIfAbsent "${IOTDB_JMX_OPTS}" "${memory_size_in_mb}")
      export IOTDB_JMX_OPTS
      ;;
    confignode)
      CONFIGNODE_JMX_OPTS=$(appendMemoryOptsIfAbsent "${CONFIGNODE_JMX_OPTS}" "${memory_size_in_mb}")
      export CONFIGNODE_JMX_OPTS
      ;;
  esac
}

# Size JVM memory from the container memory limit. Single-role containers use
# 80% of their own limit, leaving 20% for JVM native memory, thread stacks,
# page cache, and other container overhead.
function configureDockerMemoryOpts() {
  local role="$1"
  local container_memory_mb node_memory_mb

  if [[ "${IOTDB_DOCKER_RESOURCE_DETECTION}" == "false" ]]; then
    log "IoTDB docker resource detection is disabled."
    return
  fi

  container_memory_mb=$(getContainerMemoryMb)
  case "${role}" in
    datanode)
      node_memory_mb=$((container_memory_mb * 80 / 100))
      configureNodeMemoryOpts datanode "${node_memory_mb}"
      log "DataNode memory budget: $(normalizeMemoryBudgetMb "${node_memory_mb}")M"
      ;;
    confignode)
      node_memory_mb=$((container_memory_mb * 80 / 100))
      configureNodeMemoryOpts confignode "${node_memory_mb}"
      log "ConfigNode memory budget: $(normalizeMemoryBudgetMb "${node_memory_mb}")M"
      ;;
  esac

  export IOTDB_DOCKER_MEMORY_MB="${container_memory_mb}"
  log "Detected container memory: ${IOTDB_DOCKER_MEMORY_MB}M"
}

function parseNodeArgs() {
  NODE_PARAMS=""
  PRINT_GC=""
  SHOW_VERSION=""
  NODE_HEAP_DUMP_COMMAND=""

  while true; do
    case "${1:-}" in
      -c)
        if [[ "${NODE_ROLE}" == "datanode" ]]; then
          IOTDB_CONF="${2:?config folder is required after -c}"
          IOTDB_LOG_CONFIG="${IOTDB_CONF}/logback-datanode.xml"
        else
          CONFIGNODE_CONF="${2:?config folder is required after -c}"
          CONFIGNODE_LOG_CONFIG="${CONFIGNODE_CONF}/logback-confignode.xml"
        fi
        shift 2
        ;;
      -p)
        # Docker keeps node processes in the foreground and does not use pid files.
        shift 2
        ;;
      -f | -d)
        # Docker always runs node processes in the foreground.
        shift
        ;;
      -g)
        PRINT_GC="yes"
        shift
        ;;
      -H)
        NODE_HEAP_DUMP_COMMAND="${NODE_HEAP_DUMP_COMMAND} -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${2:?heap dump path is required after -H}"
        shift 2
        ;;
      -E)
        IOTDB_JVM_OPTS="${IOTDB_JVM_OPTS} -XX:ErrorFile=${2:?JVM error file is required after -E}"
        shift 2
        ;;
      -D)
        IOTDB_JVM_OPTS="${IOTDB_JVM_OPTS} -D${2:?system property is required after -D}"
        shift 2
        ;;
      -X)
        IOTDB_JVM_OPTS="${IOTDB_JVM_OPTS} -XX:${2:?JVM option is required after -X}"
        shift 2
        ;;
      -h)
        usage
        exit 0
        ;;
      -v)
        SHOW_VERSION="yes"
        shift
        ;;
      --)
        shift
        NODE_PARAMS="$*"
        break
        ;;
      "")
        break
        ;;
      *)
        NODE_PARAMS="${NODE_PARAMS} $1"
        shift
        ;;
    esac
  done
}

function configureDatanodeJvmOpts() {
  local off_heap_memory off_heap_memory_mb max_cached_buffer_size io_threads_number

  IOTDB_JMX_OPTS=$(appendJvmOptIfAbsent "${IOTDB_JMX_OPTS}" "-Xms2G" "-Xms")
  IOTDB_JMX_OPTS=$(appendJvmOptIfAbsent "${IOTDB_JMX_OPTS}" "-Xmx2G" "-Xmx")
  IOTDB_JMX_OPTS=$(appendJvmOptIfAbsent "${IOTDB_JMX_OPTS}" "-XX:MaxDirectMemorySize=512M" "-XX:MaxDirectMemorySize=")

  off_heap_memory=$(extractJvmOptValue "${IOTDB_JMX_OPTS}" "-XX:MaxDirectMemorySize=" || echo "512M")
  off_heap_memory_mb=$(parseMemoryMb "${off_heap_memory}" || echo 512)
  # Keep aligned with scripts/conf/datanode-env.sh IO_THREADS_NUMBER.
  io_threads_number=1000
  max_cached_buffer_size=$((off_heap_memory_mb * 1024 * 1024 / io_threads_number))
  OFF_HEAP_MEMORY="${off_heap_memory}"

  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -Diotdb.jmx.local=true"
  IOTDB_JMX_OPTS=$(appendJvmOptIfAbsent "${IOTDB_JMX_OPTS}" "-Djdk.nio.maxCachedBufferSize=${max_cached_buffer_size}" "-Djdk.nio.maxCachedBufferSize=")
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -XX:+CrashOnOutOfMemoryError"
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -XX:+UseAdaptiveSizePolicy"
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -Xss512k"
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -XX:SafepointTimeoutDelay=1000"
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -XX:+SafepointTimeout"
  IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

  if [[ -n "${PRINT_GC}" ]]; then
    mkdir -p "${IOTDB_LOG_DIR}"
    IOTDB_JMX_OPTS="${IOTDB_JMX_OPTS} -Xlog:gc=info,heap*=info,age*=info,safepoint=info,promotion*=info:file=${IOTDB_LOG_DIR}/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10485760"
  fi
}

function configureConfignodeJvmOpts() {
  local off_heap_memory off_heap_memory_mb max_cached_buffer_size io_threads_number

  CONFIGNODE_JMX_OPTS=$(appendJvmOptIfAbsent "${CONFIGNODE_JMX_OPTS}" "-Xms1G" "-Xms")
  CONFIGNODE_JMX_OPTS=$(appendJvmOptIfAbsent "${CONFIGNODE_JMX_OPTS}" "-Xmx1G" "-Xmx")
  CONFIGNODE_JMX_OPTS=$(appendJvmOptIfAbsent "${CONFIGNODE_JMX_OPTS}" "-XX:MaxDirectMemorySize=256M" "-XX:MaxDirectMemorySize=")

  off_heap_memory=$(extractJvmOptValue "${CONFIGNODE_JMX_OPTS}" "-XX:MaxDirectMemorySize=" || echo "256M")
  off_heap_memory_mb=$(parseMemoryMb "${off_heap_memory}" || echo 256)
  # Keep aligned with scripts/conf/confignode-env.sh IO_THREADS_NUMBER.
  io_threads_number=100
  max_cached_buffer_size=$((off_heap_memory_mb * 1024 * 1024 / io_threads_number))

  CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS} -Diotdb.jmx.local=true"
  CONFIGNODE_JMX_OPTS=$(appendJvmOptIfAbsent "${CONFIGNODE_JMX_OPTS}" "-Djdk.nio.maxCachedBufferSize=${max_cached_buffer_size}" "-Djdk.nio.maxCachedBufferSize=")
  CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS} -XX:+CrashOnOutOfMemoryError"
  CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS} -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

  if [[ -n "${PRINT_GC}" ]]; then
    mkdir -p "${CONFIGNODE_LOG_DIR}"
    CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS} -Xlog:gc=info,heap*=info,age*=info,safepoint=info,promotion*=info:file=${CONFIGNODE_LOG_DIR}/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10485760"
  fi
}

function execDatanode() {
  local java classpath
  local iotdb_params

  NODE_ROLE="datanode"
  parseNodeArgs "$@"
  java=$(findJava)
  classpath=$(buildClasspath "${IOTDB_HOME}")
  if [[ -z "${classpath}" ]]; then
    echo "Cannot find TimechoDB jars under ${IOTDB_HOME}/lib." >&2
    exit 1
  fi

  if [[ -n "${SHOW_VERSION}" ]]; then
    exec "${java}" ${IOTDB_JMX_OPTS} ${IOTDB_JVM_OPTS} \
      "-Dlogback.configurationFile=${IOTDB_CONF}/logback-tool.xml" \
      -cp "${classpath}" org.apache.iotdb.db.service.GetVersion
  fi

  illegal_access_params=()
  addJdkOpensIfNeeded "${java}"
  configureDatanodeJvmOpts
  IOTDB_JVM_OPTS="${IOTDB_JVM_OPTS} ${NODE_HEAP_DUMP_COMMAND}"
  NODE_PARAMS="-s ${NODE_PARAMS}"

  iotdb_params=(
    "-Dlogback.configurationFile=${IOTDB_LOG_CONFIG}"
    "-DIOTDB_HOME=${IOTDB_HOME}"
    "-DIOTDB_DATA_HOME=${IOTDB_DATA_HOME}"
    "-DTSFILE_HOME=${IOTDB_HOME}"
    "-DIOTDB_CONF=${IOTDB_CONF}"
    "-DTSFILE_CONF=${IOTDB_CONF}"
    "-Dname=iotdb.IoTDB"
    "-DIOTDB_LOG_DIR=${IOTDB_LOG_DIR}"
    "-DOFF_HEAP_MEMORY=${OFF_HEAP_MEMORY}"
    "-Diotdb-foreground=yes"
  )

  log "Starting TimechoDB DataNode"
  exec "${java}" \
    "${illegal_access_params[@]}" \
    "${iotdb_params[@]}" \
    ${IOTDB_JMX_OPTS} \
    ${IOTDB_JVM_OPTS} \
    -cp "${classpath}" \
    com.timecho.iotdb.DataNode \
    ${NODE_PARAMS}
}

function execConfignode() {
  local java classpath
  local iotdb_params

  NODE_ROLE="confignode"
  parseNodeArgs "$@"
  if [[ -n "${SHOW_VERSION}" ]]; then
    echo "show version is not supported in current version on ConfigNode" >&2
    exit 1
  fi

  java=$(findJava)
  classpath=$(buildClasspath "${CONFIGNODE_HOME}")
  if [[ -z "${classpath}" ]]; then
    echo "Cannot find TimechoDB jars under ${CONFIGNODE_HOME}/lib." >&2
    exit 1
  fi

  illegal_access_params=()
  addJdkOpensIfNeeded "${java}"
  configureConfignodeJvmOpts
  CONFIGNODE_JMX_OPTS="${CONFIGNODE_JMX_OPTS} ${NODE_HEAP_DUMP_COMMAND}"
  NODE_PARAMS="-s ${NODE_PARAMS}"

  iotdb_params=(
    "-Dlogback.configurationFile=${CONFIGNODE_LOG_CONFIG}"
    "-DCONFIGNODE_HOME=${CONFIGNODE_HOME}"
    "-DCONFIGNODE_DATA_HOME=${CONFIGNODE_DATA_HOME}"
    "-DTSFILE_HOME=${CONFIGNODE_HOME}"
    "-DCONFIGNODE_CONF=${CONFIGNODE_CONF}"
    "-DTSFILE_CONF=${CONFIGNODE_CONF}"
    "-Dname=iotdb.ConfigNode"
    "-DCONFIGNODE_LOGS=${CONFIGNODE_LOGS}"
    "-Diotdb-foreground=yes"
  )

  log "Starting TimechoDB ConfigNode"
  exec "${java}" \
    "${illegal_access_params[@]}" \
    "${iotdb_params[@]}" \
    ${CONFIGNODE_JMX_OPTS} \
    ${IOTDB_JVM_OPTS} \
    -cp "${classpath}" \
    com.timecho.iotdb.service.ConfigNode \
    ${NODE_PARAMS}
}

function spawnNode() {
  local role="$1"
  local pid_var="$2"
  shift 2

  case "${role}" in
    datanode)
      execDatanode "$@" &
      ;;
    confignode)
      execConfignode "$@" &
      ;;
    *)
      echo "Unknown node role: ${role}" >&2
      exit 1
      ;;
  esac

  printf -v "${pid_var}" '%s' "$!"
  log "Spawned ${role} with pid ${!pid_var}"
}

function flushDatanode() {
  if [[ -z "${DATANODE_PID}" ]] || ! kill -0 "${DATANODE_PID}" 2>/dev/null; then
    return
  fi

  log "Flushing DataNode before shutdown, timeout ${IOTDB_STOP_FLUSH_TIMEOUT}s"
  timeout "${IOTDB_STOP_FLUSH_TIMEOUT}" start-cli.sh -e "flush;" >/dev/null 2>&1 || log "DataNode flush skipped or timed out"
}

function onStop() {
  if [[ "${STOPPING}" == "1" ]]; then
    return
  fi
  STOPPING=1
  trap - SIGTERM SIGINT SIGQUIT

  log "Shutdown requested"
  flushDatanode

  if [[ -n "${DATANODE_PID}" ]] && kill -0 "${DATANODE_PID}" 2>/dev/null; then
    kill -TERM "${DATANODE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CONFIGNODE_PID}" ]] && kill -0 "${CONFIGNODE_PID}" 2>/dev/null; then
    kill -TERM "${CONFIGNODE_PID}" 2>/dev/null || true
  fi

  if [[ -n "${DATANODE_PID}" ]]; then
    wait "${DATANODE_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CONFIGNODE_PID}" ]]; then
    wait "${CONFIGNODE_PID}" 2>/dev/null || true
  fi
}

function waitSingleNode() {
  local pid="$1"
  local status=0
  wait "${pid}" || status=$?
  exit "${status}"
}

function superviseOneConfigNode() {
  spawnNode confignode CONFIGNODE_PID "$@"
  waitSingleNode "${CONFIGNODE_PID}"
}

function superviseOneDataNode() {
  spawnNode datanode DATANODE_PID "$@"
  waitSingleNode "${DATANODE_PID}"
}

if [[ -z "${start_what}" ]]; then
  usage
  exit 1
fi

case "${start_what}" in
  datanode | confignode)
    ;;
  *)
    echo "bad parameter: ${start_what}" >&2
    usage
    exit 1
    ;;
esac

"${current_path}/replace-conf-from-env.sh" "${start_what}"
configureDockerMemoryOpts "${start_what}"

trap 'onStop; exit 143' SIGTERM SIGINT SIGQUIT

case "${start_what}" in
  datanode)
    superviseOneDataNode "$@"
    ;;
  confignode)
    superviseOneConfigNode "$@"
    ;;
esac
