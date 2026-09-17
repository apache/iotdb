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

# Stop IoTDB Edge (the merged ConfigNode + DataNode process).

if [ -z "${IOTDB_HOME}" ]; then
    IOTDB_HOME="$(cd "$(dirname "$0")"/.. && pwd)"
fi

# Resolve to a physical absolute path so that the same installation reached
# through a different path still compares equal. "cd -P" is required: a plain
# "cd" collapses ".." logically, which would resolve "<symlink>/../x" against the
# symlink's parent instead of its target.
resolve_home() {
    (cd -P -- "$1" 2>/dev/null && pwd -P) || printf '%s' "$1"
}

IOTDB_HOME_RESOLVED="$(resolve_home "${IOTDB_HOME}")"

PID_FILE="${IOTDB_HOME}/edge.pid"

is_same_edge_home() {
    local command_line="$1"
    case "$command_line" in
        *"-DIOTDB_HOME=${IOTDB_HOME} "*|*"-DIOTDB_HOME=${IOTDB_HOME}")
            return 0
            ;;
        *)
            ;;
    esac
    # Fall back to comparing resolved paths, so that a start-edge.sh invoked with
    # IOTDB_HOME pointing at a symlink is still recognised here. The value is
    # delimited by the next " -D", which start-edge.sh always emits after
    # -DIOTDB_HOME. If a hand-built command line ends with -DIOTDB_HOME, the
    # extraction keeps the trailing arguments, the resolution below fails and the
    # process is simply not matched -- never matched to the wrong installation.
    local home="${command_line#*-DIOTDB_HOME=}"
    home="${home%% -D*}"
    [ -n "$home" ] && [ "$home" != "$command_line" ] || return 1
    # Only absolute values can be resolved from here. A relative one such as "."
    # is meaningful in the started process's working directory, not in ours, so
    # resolving it here could match an unrelated installation. The emptiness check
    # above matters for the same reason: "cd" succeeds on an empty argument and
    # yields our own working directory.
    case "$home" in
        /*) ;;
        *) return 1 ;;
    esac
    [ "$(resolve_home "$home")" = "${IOTDB_HOME_RESOLVED}" ]
}

is_edge_process() {
    local pid="$1"
    local command_line
    command_line=$(ps -ww -p "$pid" -o command= 2>/dev/null)
    [ -n "$command_line" ] || return 1
    printf '%s\n' "$command_line" | grep -F -- "org.apache.iotdb.edge.EdgeNode" >/dev/null || return 1
    is_same_edge_home "$command_line"
}

find_edge_processes() {
    local process_line
    local pid
    while IFS= read -r process_line; do
        printf '%s\n' "$process_line" | grep -F -- "org.apache.iotdb.edge.EdgeNode" >/dev/null || continue
        is_same_edge_home "$process_line" || continue
        pid=$(printf '%s\n' "$process_line" | awk '{print $1}')
        printf '%s\n' "$pid"
    done < <(ps -axww -o pid= -o command= 2>/dev/null)
}

stop_edge_process() {
    local pid="$1"
    if ! is_edge_process "$pid"; then
        echo "Refusing to stop PID $pid because it is not IoTDB Edge from ${IOTDB_HOME}."
        return 1
    fi
    if ! kill "$pid" 2>/dev/null; then
        echo "Failed to stop IoTDB Edge process $pid."
        return 1
    fi
    for i in $(seq 1 30); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        if ! is_edge_process "$pid"; then
            echo "Refusing to force-stop PID $pid because it no longer belongs to this IoTDB Edge installation."
            return 1
        fi
        kill -9 "$pid" 2>/dev/null
    fi
    echo "IoTDB Edge process $pid stopped."
}

PID=""
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    case "$PID" in
        ''|*[!0-9]*)
            echo "Ignoring invalid PID file ${PID_FILE}."
            PID=""
            ;;
    esac
    if [ -n "$PID" ] && ! is_edge_process "$PID"; then
        echo "Ignoring stale PID file ${PID_FILE}; PID $PID does not belong to this IoTDB Edge installation."
        PID=""
    fi
    rm -f "$PID_FILE"
fi

if [ -n "$PID" ]; then
    stop_edge_process "$PID"
    exit $?
fi

FOUND=false
while IFS= read -r PID; do
    [ -n "$PID" ] || continue
    FOUND=true
    stop_edge_process "$PID" || exit 1
done < <(find_edge_processes)

if [ "$FOUND" = false ]; then
    echo "No IoTDB Edge process from ${IOTDB_HOME} is running."
fi
exit 0
