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

import os
import shutil
from collections.abc import Callable
from pathlib import Path

import torch
import torch._dynamo
from pylru import lrucache

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
from ainode.core.exception import ModelNotExistError
from ainode.core.log import Logger
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.util.lock import ModelLockPool
from ainode.core.model.config_parser import parse_config_file
from ainode.core.model.safetensor_loader import load_weights_as_state_dict

logger = Logger()

# IoTDB 模型相关文件名
IOTDB_CONFIG_FILES = ["config.json", "configuration.json"]
IOTDB_WEIGHT_FILES = ["model.safetensors", "pytorch_model.safetensors", "model.pt", "pytorch_model.pt"]


class ModelStorage(object):
    def __init__(self):
        self._model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        if not os.path.exists(self._model_dir):
            try:
                os.makedirs(self._model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self._lock_pool = ModelLockPool()
        self._model_cache = lrucache(
            AINodeDescriptor().get_config().get_ain_model_storage_cache_size()
        )

    def register_model(self, model_id: str, uri: str):
        """
        Args:
            model_id: id of model to register
            uri: network dir path or local dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/ or /Users/admin/Desktop/model
        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        # create storage dir if not exist
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)
        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)

    def _detect_model_format(self, model_dir: str) -> tuple:
        """
        检测模型格式：legacy (model.pt + config.yaml) 或 IoTDB (config.json + safetensors)
        
        Args:
            model_dir: 模型目录路径
            
        Returns:
            (format_type, config_file, weight_file): 格式类型和对应的文件名
        """
        model_path = Path(model_dir)
        
        # 检查 IoTDB 格式
        for config_file in IOTDB_CONFIG_FILES:
            config_path = model_path / config_file
            if config_path.exists():
                # 查找权重文件
                for weight_file in IOTDB_WEIGHT_FILES:
                    weight_path = model_path / weight_file
                    if weight_path.exists():
                        logger.debug(f"检测到 IoTDB 格式: {config_file} + {weight_file}")
                        return "iotdb", config_file, weight_file
        
        # 检查 legacy 格式
        legacy_config = model_path / DEFAULT_CONFIG_FILE_NAME
        legacy_model = model_path / DEFAULT_MODEL_FILE_NAME
        if legacy_config.exists() and legacy_model.exists():
            logger.debug(f"检测到 legacy 格式: {DEFAULT_CONFIG_FILE_NAME} + {DEFAULT_MODEL_FILE_NAME}")
            return "legacy", DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
        
        return None, None, None

    def _load_iotdb_model(self, model_dir: str, config_file: str, weight_file: str, acceleration: bool) -> Callable:
        """
        加载 IoTDB 格式的模型
        
        Args:
            model_dir: 模型目录
            config_file: 配置文件名
            weight_file: 权重文件名
            acceleration: 是否启用加速
            
        Returns:
            加载的模型
        """
        config_path = os.path.join(model_dir, config_file)
        weight_path = os.path.join(model_dir, weight_file)
        
        # 检查缓存
        cache_key = f"{config_path}:{weight_path}"
        if cache_key in self._model_cache:
            model = self._model_cache[cache_key]
            if (isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration):
                return model
            else:
                model = torch.compile(model)
                self._model_cache[cache_key] = model
                return model
        
        try:
            # 解析配置文件
            config_dict = parse_config_file(config_path)
            model_type = config_dict.get("model_type", "unknown")
            
            # 根据模型类型动态导入
            if model_type == "timer":
                from ainode.model.timerxl import TimerForPrediction as ModelClass
            elif model_type == "sundial":
                from ainode.model.sundial import SundialForPrediction as ModelClass
            else:
                raise ValueError(f"不支持的模型类型: {model_type}")
            
            # 加载模型
            model = ModelClass.from_pretrained(config_path, weight_path)
            model.eval()
            
            # 转换为 TorchScript 以便缓存和部署
            try:
                # 创建示例输入
                input_length = config_dict.get("input_token_len", 96)
                example_input = torch.randn(1, input_length)
                
                # 转换为 TorchScript
                model = torch.jit.trace(model, example_input)
                logger.debug(f"成功转换 IoTDB 模型为 TorchScript: {model_type}")
            except Exception as e:
                logger.warning(f"TorchScript 转换失败，使用原生模型: {e}")
            
            # 应用加速
            if acceleration:
                try:
                    model = torch.compile(model)
                    logger.debug(f"启用模型加速: {model_type}")
                except Exception as e:
                    logger.warning(f"模型加速失败，使用普通模式: {e}")
            
            # 缓存模型
            self._model_cache[cache_key] = model
            return model
            
        except Exception as e:
            logger.error(f"加载 IoTDB 模型失败: {e}")
            raise ModelNotExistError(f"无法加载模型: {weight_path}")

    def _load_legacy_model(self, model_path: str, acceleration: bool) -> Callable:
        """
        加载 legacy 格式的模型
        
        Args:
            model_path: 模型文件路径
            acceleration: 是否启用加速
            
        Returns:
            加载的模型
        """
        if model_path in self._model_cache:
            model = self._model_cache[model_path]
            if (isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration):
                return model
            else:
                model = torch.compile(model)
                self._model_cache[model_path] = model
                return model
        else:
            if not os.path.exists(model_path):
                raise ModelNotExistError(model_path)
            else:
                model = torch.jit.load(model_path)
                if acceleration:
                    try:
                        model = torch.compile(model)
                    except Exception as e:
                        logger.warning(f"acceleration failed, fallback to normal mode: {str(e)}")
                self._model_cache[model_path] = model
                return model

    def load_model(self, model_id: str, acceleration: bool) -> Callable:
        """
        Returns:
            model: a ScriptModule contains model architecture and parameters, which can be deployed cross-platform
        """
        ain_models_dir = os.path.join(self._model_dir, f"{model_id}")
        
        with self._lock_pool.get_lock(model_id).read_lock():
            # 检测模型格式
            format_type, config_file, weight_file = self._detect_model_format(ain_models_dir)
            
            if format_type == "iotdb":
                logger.info(f"加载 IoTDB 格式模型: {model_id}")
                return self._load_iotdb_model(ain_models_dir, config_file, weight_file, acceleration)
            elif format_type == "legacy":
                logger.info(f"加载 legacy 格式模型: {model_id}")
                legacy_model_path = os.path.join(ain_models_dir, DEFAULT_MODEL_FILE_NAME)
                return self._load_legacy_model(legacy_model_path, acceleration)
            else:
                raise ModelNotExistError(f"未找到有效的模型文件: {ain_models_dir}")

    def delete_model(self, model_id: str) -> None:
        """
        Args:
            model_id: id of model to delete
        Returns:
            None
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        with self._lock_pool.get_lock(model_id).write_lock():
            if os.path.exists(storage_path):
                # 清理缓存中的所有相关条目
                keys_to_remove = []
                for cache_key in self._model_cache.keys():
                    if storage_path in cache_key:
                        keys_to_remove.append(cache_key)
                
                for key in keys_to_remove:
                    del self._model_cache[key]
                
                # 删除文件
                shutil.rmtree(storage_path)
                logger.info(f"成功删除模型: {model_id}")

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self._model_cache:
            del self._model_cache[file_path]
            
    def _get_model_directory(self, model_id: str) -> str:
        """获取模型目录路径"""
        return os.path.join(self._model_dir, model_id)