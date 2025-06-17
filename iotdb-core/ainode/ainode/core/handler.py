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
    TTrainingReq,
)
from ainode.thrift.common.ttypes import TSStatus

# only for test
from ainode.core.exception import (
    ModelLoadingError,
    ModelFormatError,
    IoTDBModelError,
    ConfigValidationError,
    WeightFileError
)

logger = Logger()


class AINodeRPCServiceHandler(IAINodeRPCService.Iface):
    def __init__(self):
        self._model_manager = ModelManager()
        self._inference_manager = InferenceManager(model_manager=self._model_manager)

    # def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
    #     return self._model_manager.register_model(req)

    # def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
    #     return self._model_manager.delete_model(req)

    # def inference(self, req: TInferenceReq) -> TInferenceResp:
    #     return self._inference_manager.inference(req)

    # def forecast(self, req: TForecastReq) -> TSStatus:
    #     return self._inference_manager.forecast(req)

    # def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
    #     return ClusterManager.get_heart_beat(req)

    # def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
    #     pass
    
    # only for test
    def registerModel(self, req: TRegisterModelReq) -> TRegisterModelResp:
        """
        注册模型，增强错误处理和日志记录
        """
        logger.info(f"开始注册模型: {req.modelId}, URI: {req.uri}")
        
        try:
            result = self._model_manager.register_model(req)
            if result.status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info(f"模型注册成功: {req.modelId}")
            else:
                logger.warning(f"模型注册失败: {req.modelId}, 状态: {result.status}")
            return result
            
        except (ModelLoadingError, ModelFormatError, IoTDBModelError, 
                ConfigValidationError, WeightFileError) as e:
            logger.error(f"模型注册失败 - 特定错误: {req.modelId}, 错误: {e}")
            from ainode.core.util.status import get_status
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, str(e))
            )
        except Exception as e:
            logger.error(f"模型注册失败 - 未知错误: {req.modelId}, 错误: {e}")
            from ainode.core.util.status import get_status
            return TRegisterModelResp(
                get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            )

    def deleteModel(self, req: TDeleteModelReq) -> TSStatus:
        """
        删除模型，增强日志记录
        """
        logger.info(f"开始删除模型: {req.modelId}")
        
        try:
            result = self._model_manager.delete_model(req)
            if result.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.info(f"模型删除成功: {req.modelId}")
            else:
                logger.warning(f"模型删除失败: {req.modelId}, 状态: {result}")
            return result
            
        except Exception as e:
            logger.error(f"模型删除失败: {req.modelId}, 错误: {e}")
            from ainode.core.util.status import get_status
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def inference(self, req: TInferenceReq) -> TInferenceResp:
        """
        执行推理，增强日志记录
        """
        logger.debug(f"开始推理: 模型ID={req.modelId}")
        
        try:
            result = self._inference_manager.inference(req)
            if result.status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.debug(f"推理成功: {req.modelId}")
            else:
                logger.warning(f"推理失败: {req.modelId}, 状态: {result.status}")
            return result
            
        except Exception as e:
            logger.error(f"推理失败: {req.modelId}, 错误: {e}")
            from ainode.core.util.status import get_status
            return TInferenceResp(
                get_status(TSStatusCode.INFERENCE_INTERNAL_ERROR, str(e)),
                []
            )

    def forecast(self, req: TForecastReq) -> TSStatus:
        """
        执行预测，增强日志记录
        """
        logger.debug(f"开始预测: 模型ID={req.modelId}")
        
        try:
            result = self._inference_manager.forecast(req)
            if result.status.code == TSStatusCode.SUCCESS_STATUS.get_status_code():
                logger.debug(f"预测成功: {req.modelId}")
            else:
                logger.warning(f"预测失败: {req.modelId}, 状态: {result.status}")
            return result
            
        except Exception as e:
            logger.error(f"预测失败: {req.modelId}, 错误: {e}")
            from ainode.core.util.status import get_status
            return get_status(TSStatusCode.INFERENCE_INTERNAL_ERROR, str(e))

    def getAIHeartbeat(self, req: TAIHeartbeatReq) -> TAIHeartbeatResp:
        return ClusterManager.get_heart_beat(req)

    def createTrainingTask(self, req: TTrainingReq) -> TSStatus:
        logger.info("训练任务暂未实现")
        from ainode.core.util.status import get_status
        return get_status(
            TSStatusCode.AINODE_INTERNAL_ERROR, 
            "Training task not implemented yet"
        )
