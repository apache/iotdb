1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1import psutil
1
1from iotdb.thrift.ainode.ttypes import TAIHeartbeatReq, TAIHeartbeatResp
1from iotdb.thrift.common.ttypes import TLoadSample
1
1
1class ClusterManager:
1    @staticmethod
1    def get_heart_beat(req: TAIHeartbeatReq) -> TAIHeartbeatResp:
1        if req.needSamplingLoad:
1            cpu_percent = psutil.cpu_percent(interval=1)
1            memory_percent = psutil.virtual_memory().percent
1            disk_usage = psutil.disk_usage("/")
1            disk_free = disk_usage.free
1            load_sample = TLoadSample(
1                cpuUsageRate=cpu_percent,
1                memoryUsageRate=memory_percent,
1                diskUsageRate=disk_usage.percent,
1                freeDiskSpace=disk_free / 1024 / 1024 / 1024,
1            )
1            return TAIHeartbeatResp(
1                heartbeatTimestamp=req.heartbeatTimestamp,
1                status="Running",
1                loadSample=load_sample,
1            )
1        else:
1            return TAIHeartbeatResp(
1                heartbeatTimestamp=req.heartbeatTimestamp, status="Running"
1            )
1