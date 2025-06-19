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

from ainode.core.constant import TSStatusCode
from ainode.core.exception import (
    ConfigValidationError,
    IoTDBModelError,
    ModelFormatError,
    ModelLoadingError,
    WeightFileError,
)
from ainode.core.log import Logger
from ainode.core.manager.cluster_manager import ClusterManager
from ainode.core.manager.inference_manager import InferenceManager
from ainode.core.manager.model_manager import ModelManager
from ainode.core.util.status import get_status
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
    TTrainingReq,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager(model_manager=self._model_manager)

    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        """
        Register a model
        """
        logger.info(f"Start register model: {req.modelId}, URI: {req.uri}")

        try:
            result = self._model_manager.register_model(req)
            if result.status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info(f"Register model successfully: {req.modelId}")
            else:
                logger.warning(
                    f"Failed to register model: {req.modelId}, with status: {result.status}"
                )
            return result

        except (
            ModelLoadingError,
            ModelFormatError,
            IoTDBModelError,
            ConfigValidationError,
            WeightFileError,
        ) as e:
            logger.error(
                f"Failed to register model: error known: {req.modelId}, with error: {e}"
            )

            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, str(e))
            )
        except Exception as e:
            logger.error(
                f"Failed to register model: unknown error: {req.modelId}, with error: {e}"
            )

            return TRegisterModelResp(
                get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            )

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        """
        Reinforce log data in delete models
        """
        logger.info(f"Start delete models: {req.modelId}")

        try:
            result = self._model_manager.delete_model(req)
            if result.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info(f"Delete models successfully: {req.modelId}")
            else:
                logger.warning(
                    f"Failed to delete models: {req.modelId}, with status: {result}"
                )
            return result

        except Exception as e:
            logger.error(f"Failed to delete models: {req.modelId}, with error: {e}")

            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        return self._inference_manager.inference(req)

    def forecast(self, req: TForecastReq) -> TSStatus:
        return self._inference_manager.forecast(req)

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        pass
