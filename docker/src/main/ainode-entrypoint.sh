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
#
# AINode Docker Entrypoint Script
# Supports configuration via environment variables for iotdb-ainode.properties
#

set -e

IOTDB_HOME="/ainode"
CONF_FILE="${IOTDB_HOME}/conf/iotdb-ainode.properties"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Initialize configuration file
init_config() {
    if [ ! -f "${CONF_FILE}" ]; then
        log "ERROR: Configuration file not found: ${CONF_FILE}"
        exit 1
    fi

    log "Initializing AINode configuration..."

    # Backup original configuration
    if [ ! -f "${CONF_FILE}.original" ]; then
        cp "${CONF_FILE}" "${CONF_FILE}.original"
    fi

    # Update configuration based on environment variables
    # Cluster configuration
    update_config "cluster_name" "${CLUSTER_NAME:-defaultCluster}"
    update_config "ain_seed_config_node" "${AIN_SEED_CONFIG_NODE:-127.0.0.1:10710}"

    # AINode service configuration
    update_config "ain_rpc_address" "${AIN_RPC_ADDRESS:-0.0.0.0}"
    update_config "ain_rpc_port" "${AIN_RPC_PORT:-10810}"

    # DataNode connection configuration
    update_config "ain_cluster_ingress_address" "${AIN_CLUSTER_INGRESS_ADDRESS:-127.0.0.1}"
    update_config "ain_cluster_ingress_port" "${AIN_CLUSTER_INGRESS_PORT:-6667}"
    update_config "ain_cluster_ingress_username" "${AIN_CLUSTER_INGRESS_USERNAME:-root}"
    update_config "ain_cluster_ingress_password" "${AIN_CLUSTER_INGRESS_PASSWORD:-root}"

    # Storage paths configuration
    update_config "ain_system_dir" "${AIN_SYSTEM_DIR:-data/ainode/system}"
    update_config "ain_models_dir" "${AIN_MODELS_DIR:-data/ainode/models}"

    # Thrift compression configuration
    update_config "ain_thrift_compression_enabled" "${AIN_THRIFT_COMPRESSION_ENABLED:-0}"

    log "Configuration initialized successfully"
}

# Update configuration item in properties file
update_config() {
    local key=$1
    local value=$2

    if grep -q "^${key}=" "${CONF_FILE}"; then
        # Update existing configuration
        sed -i "s|^${key}=.*|${key}=${value}|g" "${CONF_FILE}"
    else
        # Append new configuration
        echo "${key}=${value}" >> "${CONF_FILE}"
    fi
}

# Start AINode
start_ainode() {
    log "Starting AINode..."

    # Check if already running
    if [ -f "${IOTDB_HOME}/data/ainode.pid" ]; then
        local pid=$(cat "${IOTDB_HOME}/data/ainode.pid")
        if ps -p ${pid} > /dev/null 2>&1; then
            log "AINode is already running with PID ${pid}"
            return 0
        fi
    fi

    # Use exec to replace current process and ensure proper signal handling
    exec ${IOTDB_HOME}/sbin/start-ainode.sh
}

# Stop AINode
stop_ainode() {
    log "Stopping AINode..."
    if [ -f "${IOTDB_HOME}/sbin/stop-ainode.sh" ]; then
        ${IOTDB_HOME}/sbin/stop-ainode.sh
    else
        log "Stop script not found"
    fi
}

# Handle signals for graceful shutdown
trap stop_ainode SIGTERM SIGINT

# Main logic
case "${1:-start}" in
    start)
        init_config
        start_ainode
        ;;
    stop)
        stop_ainode
        ;;
    restart)
        stop_ainode
        sleep 10
        init_config
        start_ainode
        ;;
    status)
        if [ -f "${IOTDB_HOME}/data/ainode.pid" ]; then
            cat "${IOTDB_HOME}/data/ainode.pid"
        else
            echo "AINode is not running"
            exit 1
        fi
        ;;
    config)
        # Only generate configuration without starting (for debugging)
        init_config
        cat "${CONF_FILE}"
        ;;
    *)
        # Execute other commands directly
        exec "$@"
        ;;
esac