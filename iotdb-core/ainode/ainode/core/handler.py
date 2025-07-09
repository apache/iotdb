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

from ainode.core.log import Logger
from ainode.core.manager.cluster_manager import ClusterManager
from ainode.core.manager.inference_manager import InferenceManager
from ainode.core.manager.model_manager import ModelManager
from ainode.thrift.ainode import IAINodeRPCService
from ainode.thrift.ainode.ttypes import (
    TAIHeartbeatReq,
    TAIHeartbeatResp,
    TDeleteModelReq,
    TForecastReq,
    TInferenceReq,
    TInferenceResp,
    TRegisterModelReq,
    TRegisterModelResp,
    TShowModelsResp,
    TTrainingReq,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager(model_manager=self._model_manager)

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        return self._model_manager.delete_model(req)

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def showModels(self) -> TShowModelsResp:
        return self._model_manager.show_models()

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        pass
