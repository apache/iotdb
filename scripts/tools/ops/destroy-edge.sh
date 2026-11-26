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

set -e

if [ "$#" -gt 1 ] || { [ "$#" -eq 1 ] && [ "$1" != "-f" ]; }; then
    echo "Usage: $0 [-f]"
    exit 1
fi
if [ "${1:-}" != "-f" ]; then
    read -r -p "Do you want to clean all the data of IoTDB Edge? y/n (default n): " CLEAN_SERVICE || CLEAN_SERVICE=""
    if [[ "$CLEAN_SERVICE" != "y" && "$CLEAN_SERVICE" != "Y" ]]; then
        echo "Exiting..."
        exit 0
    fi
fi

IOTDB_HOME="${IOTDB_HOME:-$(cd "$(dirname "$0")"/../.. && pwd)}"
IOTDB_HOME="$(cd "$IOTDB_HOME" && pwd -P)"
export IOTDB_HOME
IOTDB_CONF="${IOTDB_CONF:-$IOTDB_HOME/conf}"
IOTDB_DATA_HOME="${IOTDB_DATA_HOME:-$IOTDB_HOME}"
if [ -d "$IOTDB_DATA_HOME" ]; then
    IOTDB_DATA_HOME="$(cd "$IOTDB_DATA_HOME" && pwd -P)"
fi

if [ ! -f "$IOTDB_HOME/sbin/stop-edge.sh" ]; then
    echo "Cannot find $IOTDB_HOME/sbin/stop-edge.sh. No data has been removed."
    exit 1
fi

# Stop the matching service first so Restart=on-failure cannot race with cleanup.
if command -v systemctl >/dev/null 2>&1 &&
        [ "$(systemctl show --property=PIDFile --value iotdb-edge.service 2>/dev/null)" = "$IOTDB_HOME/edge.pid" ]; then
    systemctl stop iotdb-edge
fi
bash "$IOTDB_HOME/sbin/stop-edge.sh" -f

if [ -f "$IOTDB_CONF/iotdb-system.properties" ]; then
    CN_CONFIG="$IOTDB_CONF/iotdb-system.properties"
    DN_CONFIG="$CN_CONFIG"
else
    CN_CONFIG="$IOTDB_CONF/iotdb-confignode.properties"
    DN_CONFIG="$IOTDB_CONF/iotdb-datanode.properties"
fi

read_property() {
    local config="$1" key="$2" fallback="$3"
    if [ ! -f "$config" ]; then
        printf '%s\n' "$fallback"
        return
    fi
    # Keep spaces and '=' in values; the last active property wins, as in Java.
    awk -v key="$key" -v fallback="$fallback" '
        {
            separator = index($0, "=")
            if (!separator) next
            name = substr($0, 1, separator - 1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
            if (name == key) {
                value = substr($0, separator + 1)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
            }
        }
        END { print (value == "" ? fallback : value) }
    ' "$config"
}

CLEAN_PATHS=()
add_paths() {
    local directories="$1" base="$2" path protected
    local paths=()
    IFS=';,' read -r -a paths <<< "$directories"
    for path in "${paths[@]}"; do
        path="${path#"${path%%[![:space:]]*}"}"
        path="${path%"${path##*[![:space:]]}"}"
        [ -n "$path" ] || continue
        case "$path" in
            *://*|OBJECT_STORAGE) continue ;;
            /*) ;;
            *) path="$base/$path" ;;
        esac
        # Remove a final symlink itself, not its target. Resolve directory aliases
        # before checking them so '..' and symlinked parents cannot hide a root.
        while [[ "$path" != / && "$path" == */ ]]; do path="${path%/}"; done
        if [ -L "$path" ]; then
            path="$(cd -P "$(dirname "$path")" && pwd -P)/$(basename "$path")"
        elif [ -d "$path" ]; then
            path="$(cd -P "$path" && pwd -P)"
        elif [ -e "$path" ]; then
            echo "Refusing to remove a non-directory data path: $path"
            exit 1
        else
            continue
        fi
        if [ "$path" = / ]; then
            echo "Refusing to remove the filesystem root."
            exit 1
        fi
        for protected in "$IOTDB_HOME" "$IOTDB_DATA_HOME" "$HOME"; do
            case "$protected/" in
                "$path/"*)
                    echo "Refusing to remove a home directory or its parent: $path"
                    exit 1
                    ;;
            esac
        done
        CLEAN_PATHS+=("$path")
    done
}

# The merged process still uses both sets of node directories. Preserve the
# local destroy-all behavior as well as every custom path from the node tools.
add_paths "data" "$IOTDB_HOME"
add_paths "data/datanode" "$IOTDB_DATA_HOME"
for key in cn_system_dir cn_consensus_dir; do
    case "$key" in
        cn_system_dir) fallback=data/confignode/system ;;
        cn_consensus_dir) fallback=data/confignode/consensus ;;
    esac
    value=$(read_property "$CN_CONFIG" "$key" "$fallback")
    add_paths "$value" "$IOTDB_HOME"
done
for key in dn_system_dir dn_data_dirs dn_consensus_dir dn_wal_dirs dn_tracing_dir dn_sync_dir pipe_receiver_file_dirs iot_consensus_v2_receiver_file_dirs sort_tmp_dir; do
    case "$key" in
        dn_system_dir) fallback=data/datanode/system ;;
        dn_data_dirs) fallback=data/datanode/data ;;
        dn_consensus_dir) fallback=data/datanode/consensus ;;
        dn_wal_dirs) fallback=data/datanode/wal ;;
        dn_tracing_dir) fallback=datanode/tracing ;;
        dn_sync_dir) fallback=data/datanode/sync ;;
        pipe_receiver_file_dirs) fallback=data/datanode/system/pipe/receiver ;;
        iot_consensus_v2_receiver_file_dirs) fallback=data/datanode/system/pipe/consensus/receiver ;;
        sort_tmp_dir) fallback=data/datanode/tmp ;;
    esac
    value=$(read_property "$DN_CONFIG" "$key" "$fallback")
    add_paths "$value" "$IOTDB_DATA_HOME"
done

# Validate every target before deleting any data, and finish before reporting success.
for path in "${CLEAN_PATHS[@]}"; do
    rm -rf -- "$path"
done
echo "IoTDB Edge clean done ..."
