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

from yaml import YAMLError

from ainode.core.constant import BuiltInModelType, TSStatusCode
from ainode.core.exception import (
    BadConfigValueError,
    BuiltInModelNotSupportError,
    InvalidUriError,
)
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import fetch_built_in_model
from ainode.core.model.model_storage import ModelStorage
from ainode.core.util.status import get_status
from ainode.thrift.ainode.ttypes import (
    TDeleteModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
)
from ainode.thrift.common.ttypes import TSStatus

# create model新增：json格式解析
import json
import threading
import time
import os
from ainode.core.client import ClientManager
from ainode.core.config import AINodeDescriptor
from ainode.core.util.model_utils import (
    detect_model_format, 
    validate_iotdb_model_config,
    get_model_info_from_config,
    validate_model_name
)

logger = Logger()


class ModelManager:
    def __init__(self):
        self.model_storage = ModelStorage()
        self._model_status_cache = {}  # 缓存模型状态
        self._status_lock = threading.Lock()

    def register_model(self, req: TRegisterModelReq) -> TRegisterModelResp:
        
        # TODO：验证模型名称是否受到支持
        if not validate_model_name(req.modelId):
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, "Invalid model name")
            )
        
        # 更新模型状态为加载中
        self._update_model_status(req.modelId, "LOADING", "Model registration started")
        
        logger.info(f"register model {req.modelId} from {req.uri}")
        try:
            # 检测模型格式
            from ainode.core.util.model_utils import parse_model_uri
            is_network, parsed_uri = parse_model_uri(req.uri)
            
            # 使用现有的模型存储注册机制
            configs, attributes = self.model_storage.register_model(
                req.modelId, req.uri
            )
            
            # 检查是否为IoTDB格式并验证
            try:
                model_dir = self.model_storage._get_model_directory(req.modelId)
                format_type, config_file, weight_file = detect_model_format(model_dir)
                
                if format_type == "iotdb":
                    config_path = os.path.join(model_dir, config_file)
                    model_info = get_model_info_from_config(config_path)
                    
                    # 验证IoTDB配置
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_dict = json.load(f)
                    
                    if not validate_iotdb_model_config(config_dict):
                        self._update_model_status(req.modelId, "ERROR", "Invalid IoTDB model configuration")
                        self.model_storage.delete_model(req.modelId)
                        return TRegisterModelResp(
                            get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, "Invalid IoTDB model configuration")
                        )
                    
                    # 更新attributes以包含IoTDB模型信息
                    import ast
                    try:
                        attr_dict = ast.literal_eval(attributes) if attributes else {}
                    except:
                        attr_dict = {}
                    
                    attr_dict.update({
                        "format": "iotdb",
                        "model_type": model_info["model_type"],
                        "architecture": model_info.get("architecture", ""),
                        "task": model_info.get("task", "forecasting")
                    })
                    attributes = str(attr_dict)
                    
                    logger.info(f"Successfully registered IoTDB model: {req.modelId}, type: {model_info['model_type']}")
                
            except Exception as e:
                logger.warning(f"Format detection failed, treating as legacy model: {e}")
            
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
            logger.warning(e)
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
            ClientManager().borrow_config_node_client().update_model_info(
                model_id=model_id,
                model_status=status_code,
                attribute=message,
                ainode_id=[AINodeDescriptor().get_config().get_ainode_id()]
            )
            
            logger.info(f"Model {model_id} status updated to {status}: {message}")
            
        except Exception as e:
            logger.error(f"Failed to update model status for {model_id}: {e}")

    def get_model_status(self, model_id: str) -> dict:
        """获取模型状态"""
        with self._status_lock:
            return self._model_status_cache.get(model_id, {"status": "UNKNOWN", "message": ""})

    def load_model(self, model_id: str, acceleration: bool = False) -> Callable:
        logger.info(f"load model {model_id}")
        return self.model_storage.load_model(model_id, acceleration)

    @staticmethod
    def load_built_in_model(model_id: str, attributes: {}):
        model_id = model_id.lower()
        if model_id not in BuiltInModelType.values():
            raise BuiltInModelNotSupportError(model_id)
        return fetch_built_in_model(model_id, attributes)
