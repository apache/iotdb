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

from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.cluster_manager import ClusterManager
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.manager.model_manager import get_model_manager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.gpu_mapping import get_available_devices
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
        self._model_manager = get_model_manager()
        self._inference_manager = InferenceManager()

    def stop(self) -> None:
        logger.info("Stopping the RPC service handler of IoTDB-AINode...")
        self._inference_manager.stop()

    def stopAINode(self) -> TSStatus:
        self._ainode.stop()
        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        return self._model_manager.delete_model(req)

    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
        return self._model_manager.show_models(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        if not self._model_manager.is_model_registered(req.existingModelId):
            return TSStatus(
                code=TSStatusCode.MODEL_NOT_FOUND_ERROR.value,
                message=f"Model [{req.existingModelId}] is not supported. You can use 'SHOW MODELS' to retrieve the available models.",
            )

        available_devices = get_available_devices()
        for device_id in req.deviceIdList:
            if device_id not in available_devices:
                return TSStatus(
                    code=TSStatusCode.INVALID_URI_ERROR.value,
                    message=f"Device ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
                )

        return self._inference_manager.load_model(req)

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        if not self._model_manager.is_model_registered(req.modelId):
            return TSStatus(
                code=TSStatusCode.MODEL_NOT_FOUND_ERROR.value,
                message=f"Model [{req.modelId}] is not supported. You can use 'SHOW MODELS' to retrieve the available models.",
            )

        available_devices = get_available_devices()
        for device_id in req.deviceIdList:
            if device_id not in available_devices:
                return TSStatus(
                    code=TSStatusCode.INVALID_URI_ERROR.value,
                    message=f"Device ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
                )

        return self._inference_manager.unload_model(req)

    def showLoadedModels(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
        available_devices = get_available_devices()
        for device_id in req.deviceIdList:
            if device_id not in available_devices:
                status = TSStatus(
                    code=TSStatusCode.INVALID_URI_ERROR.value,
                    message=f"Device ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
                )
                return TShowLoadedModelsResp(status=status, deviceLoadedModelsMap={})
        return self._inference_manager.show_loaded_models(req)

    def showAIDevices(self) -> TShowAIDevicesResp:
        return TShowAIDevicesResp(
            status=TSStatus(code=TSStatusCode.SUCCESS_STATUS.value),
            deviceIdList=get_available_devices(),
        )

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        pass
