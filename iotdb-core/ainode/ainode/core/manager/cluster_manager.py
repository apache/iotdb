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
import threading
import time

import psutil

from ainode.core.config import AINodeDescriptor
from ainode.core.log import Logger
from ainode.thrift.ainode.ttypes import TAIHeartbeatReq, TAIHeartbeatResp
from ainode.thrift.common.ttypes import TLoadSample

logger = Logger()


class ClusterManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._node_status = "STARTING"
            self._start_time = time.time()
            self._last_heartbeat = 0
            self._heartbeat_count = 0
            self._initialized = True

    @staticmethod
    def get_heart_beat(req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        """
        Enhanced heartbeat response with additional node information
        """
        instance = ClusterManager()
        instance._last_heartbeat = time.time()
        instance._heartbeat_count += 1
        instance._node_status = "RUNNING"

        logger.debug(
            f"Heartbeat request #{instance._heartbeat_count}, needSamplingLoad: {req.needSamplingLoad}"
        )

        if req.needSamplingLoad:
            try:
                # System load metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory_info = psutil.virtual_memory()
                memory_percent = memory_info.percent
                disk_usage = psutil.disk_usage("/")
                disk_free = disk_usage.free

                load_sample = TLoadSample(
                    cpuUsageRate=cpu_percent,
                    memoryUsageRate=memory_percent,
                    diskUsageRate=disk_usage.percent,
                    freeDiskSpace=disk_free / 1024 / 1024 / 1024,  # GB
                )

                logger.debug(
                    f"System load - CPU: {cpu_percent:.1f}%, "
                    f"Memory: {memory_percent:.1f}%, "
                    f"Disk Usage: {disk_usage.percent:.1f}%, "
                    f"Free Space: {disk_free / 1024 / 1024 / 1024:.1f}GB"
                )

                return TAIHeartbeatResp(
                    heartbeatTimestamp=req.heartbeatTimestamp,
                    status=instance._node_status,
                    loadSample=load_sample,
                )
            except Exception as e:
                logger.error(f"Failed to retrieve system load metrics: {e}")
                # Return basic heartbeat if system load cannot be retrieved
                return TAIHeartbeatResp(
                    heartbeatTimestamp=req.heartbeatTimestamp,
                    status="RUNNING_WITH_ERROR",
                )
        else:
            return TAIHeartbeatResp(
                heartbeatTimestamp=req.heartbeatTimestamp, status=instance._node_status
            )

    def get_node_info(self) -> dict:
        """Retrieve detailed node information"""
        try:
            config = AINodeDescriptor().get_config()
            uptime = time.time() - self._start_time

            return {
                "node_id": config.get_ainode_id(),
                "cluster_name": config.get_cluster_name(),
                "status": self._node_status,
                "uptime_seconds": uptime,
                "heartbeat_count": self._heartbeat_count,
                "last_heartbeat": self._last_heartbeat,
                "rpc_address": config.get_ain_inference_rpc_address(),
                "rpc_port": config.get_ain_inference_rpc_port(),
                "version": config.get_version_info(),
                "build": config.get_build_info(),
            }
        except Exception as e:
            logger.error(f"Failed to retrieve node information: {e}")
            return {"error": str(e)}

    def set_node_status(self, status: str):
        """Set the status of the current node"""
        self._node_status = status
        logger.info(f"Node status updated to: {status}")

    def get_system_metrics(self) -> dict:
        """Retrieve system-level metrics"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage("/")

            return {
                "cpu": {
                    "usage_percent": cpu_percent,
                    "count": psutil.cpu_count(),
                },
                "memory": {
                    "total_gb": memory.total / 1024 / 1024 / 1024,
                    "available_gb": memory.available / 1024 / 1024 / 1024,
                    "usage_percent": memory.percent,
                },
                "disk": {
                    "total_gb": disk.total / 1024 / 1024 / 1024,
                    "free_gb": disk.free / 1024 / 1024 / 1024,
                    "usage_percent": disk.percent,
                },
                "timestamp": time.time(),
            }
        except Exception as e:
            logger.error(f"Failed to retrieve system metrics: {e}")
            return {"error": str(e)}
