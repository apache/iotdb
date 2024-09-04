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

from iotdb.ainode.constant import TSStatusCode
from iotdb.ainode.inference import inference_with_registered_model, inference_with_built_in_model, inference
from iotdb.ainode.log import logger
from iotdb.ainode.manager.cluster_manager import ClusterManager
from iotdb.ainode.manager.model_manager import ModelManager
from iotdb.ainode.parser import (parse_inference_request)
from iotdb.ainode.serde import convert_to_binary
from iotdb.ainode.util import get_status
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (TDeleteModelReq, TRegisterModelReq,
                                        TAIHeartbeatReq, TInferenceReq, TInferenceResp)


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        self._model_manager = ModelManager()

    def registerModel(self, req: TRegisterModelReq):
        self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq):
        self._model_manager.delete_model(req)

    def inference(self, req: TInferenceReq):
        return inference(req, self._model_manager)


    def getAIHeartbeat(self, req: TAIHeartbeatReq):
        return ClusterManager.get_heart_beat(req)
