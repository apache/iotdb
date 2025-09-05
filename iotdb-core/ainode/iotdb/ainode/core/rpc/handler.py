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
import torch.cuda

from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.cluster_manager import ClusterManager
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.gpu_mapping import get_available_gpus
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (
    TAIHeartbeatReq,
    TAIHeartbeatResp,
    TDeleteModelReq,
    TForecastReq,
    TInferenceReq,
    TInferenceResp,
    TLoadModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
    TShowAIDevicesResp,
    TShowLoadedModelsReq,
    TShowLoadedModelsResp,
    TShowModelsReq,
    TShowModelsResp,
    TTrainingReq,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self, ainode):
        self._ainode = ainode
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager()

    def stop(self) -> None:
        logger.info("Stopping the RPC service handler of IoTDB-AINode...")
        self._inference_manager.shutdown()

    def stopAINode(self) -> TSStatus:
        self._ainode.stop()
        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        return self._model_manager.register_model(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        return self._inference_manager.loadModel(req)

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        return self._inference_manager.unloadModel(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        return self._model_manager.delete_model(req)

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
        return self._model_manager.show_models(req)

    def showLoadedModels(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
        pass

    def showAIDevices(self) -> TShowAIDevicesResp:
        device_id_list = get_available_gpus()
        device_id_list = [str(device_id) for device_id in device_id_list]
        device_id_list.append("cpu")
        return TShowAIDevicesResp(deviceIdList=device_id_list)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        pass
