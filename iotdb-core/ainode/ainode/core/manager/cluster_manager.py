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
import time
import threading

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
        增强的心跳响应，包含更多节点信息
        """
        instance = ClusterManager()
        instance._last_heartbeat = time.time()
        instance._heartbeat_count += 1
        instance._node_status = "RUNNING"
        
        logger.debug(f"心跳请求 #{instance._heartbeat_count}, needSamplingLoad: {req.needSamplingLoad}")
        
        if req.needSamplingLoad:
            try:
                # 系统负载信息
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
                
                logger.debug(f"系统负载 - CPU: {cpu_percent:.1f}%, "
                           f"内存: {memory_percent:.1f}%, "
                           f"磁盘使用: {disk_usage.percent:.1f}%, "
                           f"可用空间: {disk_free / 1024 / 1024 / 1024:.1f}GB")
                
                return TAIHeartbeatResp(
                    heartbeatTimestamp=req.heartbeatTimestamp,
                    status=instance._node_status,
                    loadSample=load_sample,
                )
            except Exception as e:
                logger.error(f"获取系统负载信息失败: {e}")
                # 如果获取负载信息失败，返回基本心跳
                return TAIHeartbeatResp(
                    heartbeatTimestamp=req.heartbeatTimestamp, 
                    status="RUNNING_WITH_ERROR"
                )
        else:
            return TAIHeartbeatResp(
                heartbeatTimestamp=req.heartbeatTimestamp, 
                status=instance._node_status
            )

    def get_node_info(self) -> dict:
        """获取节点详细信息"""
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
            logger.error(f"获取节点信息失败: {e}")
            return {"error": str(e)}

    def set_node_status(self, status: str):
        """设置节点状态"""
        self._node_status = status
        logger.info(f"节点状态更新为: {status}")

    def get_system_metrics(self) -> dict:
        """获取系统指标"""
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
                "timestamp": time.time()
            }
        except Exception as e:
            logger.error(f"获取系统指标失败: {e}")
            return {"error": str(e)}
