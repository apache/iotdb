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
from iotdb.ainode.core.manager.device_manager import DeviceManager
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.thrift.ainode import IAINodeRPCService
from iotdb.thrift.ainode.ttypes import (
    TAIHeartbeatReq,
    TAIHeartbeatResp,
    TDeleteModelReq,
    TForecastReq,
    TForecastResp,
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
    TTuningReq,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self, ainode):
        self._ainode = ainode
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager()
        self._backend = DeviceManager()

    # ==================== Cluster Management ====================

    def stop(self):
        logger.info("Stopping the RPC service handler of IoTDB-AINode...")
        self._inference_manager.stop()

    def stopAINode(self) -> TSStatus:
        self._ainode.stop()
        return get_status(TSStatusCode.SUCCESS_STATUS, "AINode stopped successfully.")

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def showAIDevices(self) -> TShowAIDevicesResp:
        device_id_map = {"cpu": "cpu"}
        for device_id in self._backend.device_ids():
            device_id_map[str(device_id)] = self._backend.type.value
        return TShowAIDevicesResp(
            status=TSStatus(code=TSStatusCode.SUCCESS_STATUS.value),
            deviceIdMap=device_id_map,
        )

    # ==================== Model Management ====================

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        return self._model_manager.register_model(req)

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        return self._model_manager.delete_model(req)

    def showModels(self, req: TShowModelsReq) -> TShowModelsResp:
        return self._model_manager.show_models(req)

    def loadModel(self, req: TLoadModelReq) -> TSStatus:
        status = self._ensure_model_is_registered(req.existingModelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        status = self._ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        return self._inference_manager.load_model(
            req.existingModelId,
            [self._backend.torch_device(device_id) for device_id in req.deviceIdList],
        )

    def unloadModel(self, req: TUnloadModelReq) -> TSStatus:
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        status = self._ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return status
        return self._inference_manager.unload_model(
            req.modelId,
            [self._backend.torch_device(device_id) for device_id in req.deviceIdList],
        )

    def showLoadedModels(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
        status = self._ensure_device_id_is_available(req.deviceIdList)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TShowLoadedModelsResp(status=status, deviceLoadedModelsMap={})
        return self._inference_manager.show_loaded_models(
            [self._backend.torch_device(device_id) for device_id in req.deviceIdList]
        )

    def _ensure_model_is_registered(self, model_id: str) -> TSStatus:
        if not self._model_manager.is_model_registered(model_id):
            return TSStatus(
                code=TSStatusCode.MODEL_NOT_EXIST_ERROR.value,
                message=f"Model [{model_id}] is not registered yet. You can use 'SHOW MODELS' to retrieve the available models.",
            )
        return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)

    # ==================== Inference ====================

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TInferenceResp(status, [])
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TForecastResp:
        status = self._ensure_model_is_registered(req.modelId)
        if status.code != TSStatusCode.SUCCESS_STATUS.value:
            return TForecastResp(status, [])
        return self._inference_manager.forecast(req)

    # ==================== Internal API ====================

    def _ensure_device_id_is_available(self, device_id_list: list[str]) -> TSStatus:
        """
        Ensure that the device IDs in the provided list are available.
        """
        available_devices = self._backend.device_ids()
        for device_id in device_id_list:
            try:
                if device_id == "cpu":
                    continue
                if int(device_id) not in available_devices:
                    raise ValueError(f"Invalid device ID [{device_id}]")
            except (TypeError, ValueError):
                return TSStatus(
                    code=TSStatusCode.UNAVAILABLE_AI_DEVICE_ERROR.value,
                    message=f"AIDevice ID [{device_id}] is not available. You can use 'SHOW AI_DEVICES' to retrieve the available devices.",
                )
        return TSStatus(code=TSStatusCode.SUCCESS_STATUS.value)

    # ==================== Tuning ====================

    def createTuningTask(self, req: TTuningReq) -> TSStatus:
        pass
