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
from typing import Callable
import threading
import time

from yaml import YAMLError

from ainode.core.constant import BuiltInModelType, TSStatusCode
from ainode.core.exception import (
    BadConfigValueError,
    BuiltInModelNotSupportError,
    InvalidUriError,
    ModelLoadingError,
    ModelFormatError,
    ConfigValidationError,
    WeightFileError
)
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import fetch_built_in_model
from ainode.core.model.model_storage import ModelStorage
from ainode.core.util.status import get_status
from ainode.core.client import ClientManager
from ainode.core.config import AINodeDescriptor
from ainode.thrift.ainode.ttypes import (
    TDeleteModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()


class ModelManager:
    def __init__(self):
        self.model_storage = ModelStorage()
        self._model_status_cache = {}  # 缓存模型状态
        self._status_lock = threading.Lock()

    def register_model(self, req: TRegisterModelReq) -> TRegisterModelResp:
        logger.info(f"register model {req.modelId} from {req.uri}")
        
        # 验证模型名称
        if not self._validate_model_name(req.modelId):
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, "Invalid model name")
            )
        
        # 更新模型状态为加载中
        self._update_model_status(req.modelId, "LOADING", "Model registration started")
        
        try:
            configs, attributes = self.model_storage.register_model(
                req.modelId, req.uri
            )
            
            # 验证注册后的模型文件
            try:
                validation_result = self.model_storage.validate_model_files(req.modelId)
                if not validation_result["valid"]:
                    error_msg = f"Model validation failed: {validation_result['errors']}"
                    logger.error(error_msg)
                    self._update_model_status(req.modelId, "ERROR", error_msg)
                    self.model_storage.delete_model(req.modelId)
                    return TRegisterModelResp(
                        get_status(TSStatusCode.INVALID_URI_ERROR, error_msg)
                    )
                
                # 记录模型格式信息
                model_format = validation_result.get("format", "unknown")
                logger.info(f"Model {req.modelId} registered, with format: {model_format}")
                
            except Exception as e:
                logger.warning(f"Register model without validation: {e}")
            
            # 更新模型状态为活跃
            self._update_model_status(req.modelId, "ACTIVE", "Model registration completed")
            
            return TRegisterModelResp(
                get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes
            )
            
        except InvalidUriError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, e.message)
            )
        except BadConfigValueError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, e.message)
            )
        except (ModelLoadingError, ModelFormatError, ConfigValidationError, WeightFileError) as e:
            logger.warning(f"IoTDB model failed: {e}")
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, str(e))
            )
        except YAMLError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", "YAML parsing error")
            self.model_storage.delete_model(req.modelId)
            if hasattr(e, "problem_mark"):
                mark = e.problem_mark
                return TRegisterModelResp(
                    get_status(
                        TSStatusCode.INVALID_INFERENCE_CONFIG,
                        f"An error occurred while parsing the yaml file, "
                        f"at line {mark.line + 1} column {mark.column + 1}.",
                    )
                )
            return TRegisterModelResp(
                get_status(
                    TSStatusCode.INVALID_INFERENCE_CONFIG,
                    f"An error occurred while parsing the yaml file",
                )
            )
        except Exception as e:
            logger.warning(f"未知错误: {e}")
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e)))
        
    def delete_model(self, req: TDeleteModelReq) -> TSStatus:
        logger.info(f"delete model {req.modelId}")
        try:
            # 更新模型状态为非活跃
            self._update_model_status(req.modelId, "INACTIVE", "Model deletion started")
            
            self.model_storage.delete_model(req.modelId)
            
            # 从状态缓存中移除
            with self._status_lock:
                self._model_status_cache.pop(req.modelId, None)
            
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", str(e))
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def load_model(self, model_id: str, acceleration: bool = False) -> Callable:
        logger.info(f"load model {model_id}")
        try:
            model = self.model_storage.load_model(model_id, acceleration)
            logger.info(f"Model {model_id} Loaded")
            return model
        except Exception as e:
            logger.error(f"Failed to load model {model_id} : {e}")
            self._update_model_status(model_id, "ERROR", f"Model loading failed: {str(e)}")
            raise
        
    def save_model(self, model_id: str, save_directory: str = None) -> TSStatus:
        """
        保存模型使用save_pretrained
        """
        logger.info(f"Saving model {model_id}")
        try:
            success = self.model_storage.save_model_with_save_pretrained(
                model_id, self.model_storage.load_model(model_id, acceleration=False), save_directory
            )
            
            if success:
                return get_status(TSStatusCode.SUCCESS_STATUS, f"Model {model_id} saved successfully")
            else:
                return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, f"Failed to save model {model_id}")
                
        except Exception as e:
            logger.error(f"Save model failed: {e}")
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def clone_model(self, source_model_id: str, target_model_id: str) -> TSStatus:
        """
        克隆模型使用save_pretrained + from_pretrained
        """
        logger.info(f"Cloning model {source_model_id} to {target_model_id}")
        try:
            success = self.model_storage.clone_model_with_save_load(source_model_id, target_model_id)
            
            if success:
                self._update_model_status(target_model_id, "ACTIVE", "Model cloned successfully")
                return get_status(TSStatusCode.SUCCESS_STATUS, f"Model cloned: {source_model_id} -> {target_model_id}")
            else:
                return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, f"Failed to clone model {source_model_id}")
                
        except Exception as e:
            logger.error(f"Clone model failed: {e}")
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        return self.model_storage.get_ckpt_path(model_id)
    
    def _validate_model_name(self, model_name: str) -> bool:
        """
        验证模型名称的有效性
        
        Args:
            model_name: 模型名称
            
        Returns:
            是否有效
        """
        if not model_name:
            return False
            
        # 允许字母、数字、下划线、连字符
        import re
        pattern = r'^[a-zA-Z0-9_-]+$'
        
        if re.match(pattern, model_name):
            logger.debug(f"Pass valisation: {model_name}")
            return True
        else:
            logger.error(f"Illegal model name: {model_name}")
            return False

    def _update_model_status(self, model_id: str, status: str, message: str = ""):
        """更新模型状态并通知ConfigNode"""
        try:
            with self._status_lock:
                self._model_status_cache[model_id] = {
                    "status": status,
                    "message": message,
                    "timestamp": time.time()
                }
            
            # 映射状态到数字码
            status_code_map = {
                "LOADING": 0,
                "ACTIVE": 1, 
                "INACTIVE": 2,
                "ERROR": 3
            }
            
            status_code = status_code_map.get(status, 3)
            
            # 通知ConfigNode
            try:
                ClientManager().borrow_config_node_client().update_model_info(
                    model_id=model_id,
                    model_status=status_code,
                    attribute=message,
                    ainode_id=[AINodeDescriptor().get_config().get_ainode_id()]
                )
                logger.info(f"Model {model_id} status updated to {status}: {message}")
            except Exception as e:
                logger.warning(f"Failed to notify ConfigNode about model status: {e}")
            
        except Exception as e:
            logger.error(f"Failed to update model status for {model_id}: {e}")

    def get_model_status(self, model_id: str) -> dict:
        """获取模型状态"""
        with self._status_lock:
            return self._model_status_cache.get(model_id, {"status": "UNKNOWN", "message": ""})

    def list_models(self) -> dict:
        """列出所有模型及其状态"""
        with self._status_lock:
            return dict(self._model_status_cache)

    @staticmethod
    def load_built_in_model(model_id: str, attributes: {}):
        model_id = model_id.lower()
        if model_id not in BuiltInModelType.values():
            raise BuiltInModelNotSupportError(model_id)
        return fetch_built_in_model(model_id, attributes)
