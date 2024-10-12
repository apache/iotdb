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
import psutil

from iotdb.thrift.ainode.ttypes import TAIHeartbeatResp, TAIHeartbeatReq
from iotdb.thrift.common.ttypes import TLoadSample


class ClusterManager:
    @staticmethod
    def get_heart_beat(req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        if req.needSamplingLoad:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage('/')
            disk_free = disk_usage.free
            load_sample = TLoadSample(cpuUsageRate=cpu_percent,
                                      memoryUsageRate=memory_percent,
                                      diskUsageRate=disk_usage.percent,
                                      freeDiskSpace=disk_free / 1024 / 1024 / 1024)
            return TAIHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running",
                                    loadSample=load_sample)
        else:
            return TAIHeartbeatResp(heartbeatTimestamp=req.heartbeatTimestamp,
                                    status="Running")
