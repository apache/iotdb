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
import json
from collections.abc import Callable
from pathlib import Path

import torch
import torch._dynamo
from pylru import lrucache

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import (
    DEFAULT_CONFIG_FILE_NAME, 
    DEFAULT_MODEL_FILE_NAME,
    IOTDB_CONFIG_FILES,
    WEIGHT_FORMAT_PRIORITY
)
from ainode.core.exception import ModelNotExistError
from ainode.core.log import Logger
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.util.lock import ModelLockPool

from ainode.core.model.config_parser import (
    parse_config_file, 
    convert_iotdb_config_to_ainode_format,
    validate_iotdb_config,
    detect_config_format
)
from ainode.core.model.safetensor_loader import (
    load_weights_as_state_dict,
    load_weights_for_from_pretrained,
    get_available_weight_files
)

logger = Logger()


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
            uri: network dir path or local dir path of model to register, where model files are located
        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        # create storage dir if not exist
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        
        # 使用通用的文件名，实际格式由model_factory自动检测
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)
        
        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)
    
    def _detect_local_model_format(self, model_dir: str) -> tuple:
            """
            检测本地模型格式，集成 config_parser
            
            Returns:
                (format_type, config_file, weight_file)
            """
            model_path = Path(model_dir)
            
            # 使用 config_parser 检测配置格式
            for config_file in IOTDB_CONFIG_FILES:
                config_path = model_path / config_file
                if config_path.exists():
                    try:
                        # 使用 config_parser 验证配置
                        config_format = detect_config_format(config_path)
                        if config_format == "iotdb":
                            config_dict = parse_config_file(config_path)
                            if validate_iotdb_config(config_dict):
                                # 查找权重文件
                                for weight_file in WEIGHT_FORMAT_PRIORITY:
                                    weight_path = model_path / weight_file
                                    if weight_path.exists():
                                        logger.info(f"检测到IoTDB格式: {config_file} + {weight_file}")
                                        return "iotdb", config_file, weight_file
                    except Exception as e:
                        logger.warning(f"配置文件验证失败: {config_path}, 错误: {e}")
                        continue
            
            # 检查legacy格式
            legacy_config = model_path / DEFAULT_CONFIG_FILE_NAME
            legacy_model = model_path / DEFAULT_MODEL_FILE_NAME
            if legacy_config.exists() and legacy_model.exists():
                logger.info("检测到legacy格式")
                return "legacy", DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
            
            return None, None, None

    def _load_iotdb_model(self, model_dir: str, config_file: str, weight_file: str, acceleration: bool) -> Callable:
        """
        加载IoTDB格式模型，完全集成 config_parser 和 safetensor_loader
        """
        config_path = os.path.join(model_dir, config_file)
        weight_path = os.path.join(model_dir, weight_file)
        
        # 使用复合键进行缓存
        cache_key = f"{config_path}:{weight_path}"
        
        if cache_key in self._model_cache:
            model = self._model_cache[cache_key]
            if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
                return model
            else:
                model = torch.compile(model)
                self._model_cache[cache_key] = model
                return model
        
        try:
            # 使用 config_parser 解析配置文件
            config_dict = parse_config_file(config_path)
            
            # 验证配置
            if not validate_iotdb_config(config_dict):
                raise ValueError(f"无效的IoTDB配置: {config_path}")
            
            model_type = config_dict.get("model_type", "unknown")
            logger.info(f"加载IoTDB模型: {model_type}")
            
            # 使用 safetensor_loader 验证权重文件
            try:
                # 验证权重文件可以正确加载
                weights = load_weights_for_from_pretrained(model_dir, weight_file)
                logger.debug(f"权重文件验证成功，包含 {len(weights)} 个参数")
            except Exception as e:
                logger.error(f"权重文件验证失败: {e}")
                raise ModelNotExistError(f"权重文件损坏或不兼容: {weight_path}")
            
            # 根据模型类型动态导入
            if model_type == "timer":
                from ainode.core.model.timerxl.modeling_timer import TimerForPrediction as ModelClass
            elif model_type == "sundial":
                from ainode.core.model.sundial.modeling_sundial import SundialForPrediction as ModelClass
            else:
                raise ValueError(f"不支持的模型类型: {model_type}")
            
            # 使用from_pretrained加载模型
            # 传递目录路径，让模型类自己处理配置和权重文件
            model = ModelClass.from_pretrained(model_dir)
            model.eval()
            
            # 尝试转换为TorchScript
            try:
                input_length = config_dict.get("input_token_len", 96)
                example_input = torch.randn(1, input_length)
                traced_model = torch.jit.trace(model, example_input)
                logger.debug(f"成功转换IoTDB模型为TorchScript: {model_type}")
                model = traced_model
            except Exception as e:
                logger.warning(f"TorchScript转换失败，使用原生模型: {e}")
            
            # 应用加速
            if acceleration:
                try:
                    model = torch.compile(model)
                    logger.debug(f"启用模型加速: {model_type}")
                except Exception as e:
                    logger.warning(f"模型加速失败: {e}")
            
            # 缓存模型
            self._model_cache[cache_key] = model
            return model
            
        except Exception as e:
            logger.error(f"加载IoTDB模型失败: {e}")
            raise ModelNotExistError(f"无法加载模型: {weight_path}")
        
        
    def _load_legacy_model(self, model_path: str, acceleration: bool) -> Callable:
        """
        加载legacy格式模型
        """
        if model_path in self._model_cache:
            model = self._model_cache[model_path]
            if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
                return model
            else:
                model = torch.compile(model)
                self._model_cache[model_path] = model
                return model
        else:
            if not os.path.exists(model_path):
                raise ModelNotExistError(model_path)
            
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
        加载模型，支持IoTDB和legacy格式自动检测
        
        Returns:
            model: a model that can be deployed cross-platform
        """
        model_dir = os.path.join(self._model_dir, f"{model_id}")
        
        with self._lock_pool.get_lock(model_id).read_lock():
            # 检测模型格式
            format_type, config_file, weight_file = self._detect_local_model_format(model_dir)
            
            if format_type == "iotdb":
                logger.info(f"加载IoTDB格式模型: {model_id}")
                return self._load_iotdb_model(model_dir, config_file, weight_file, acceleration)
            elif format_type == "legacy":
                logger.info(f"加载legacy格式模型: {model_id}")
                legacy_model_path = os.path.join(model_dir, DEFAULT_MODEL_FILE_NAME)
                return self._load_legacy_model(legacy_model_path, acceleration)
            else:
                # 如果格式检测失败，尝试使用原有逻辑作为fallback
                legacy_model_path = os.path.join(model_dir, DEFAULT_MODEL_FILE_NAME)
                if os.path.exists(legacy_model_path):
                    logger.warning(f"格式检测失败，尝试legacy加载: {model_id}")
                    return self._load_legacy_model(legacy_model_path, acceleration)
                else:
                    # 获取可用的权重文件信息用于错误报告
                    available_files = get_available_weight_files(model_dir)
                    logger.error(f"模型加载失败，可用文件: {available_files}")
                    raise ModelNotExistError(f"未找到有效的模型文件: {model_dir}")

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
                keys_to_remove = []
                for cache_key in list(self._model_cache.keys()):
                    if storage_path in str(cache_key):
                        keys_to_remove.append(cache_key)
                
                for key in keys_to_remove:
                    del self._model_cache[key]
                for file_name in os.listdir(storage_path):
                    self._remove_from_cache(os.path.join(storage_path, file_name))
                shutil.rmtree(storage_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self._model_cache:
            del self._model_cache[file_path]

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        return os.path.join(self._model_dir, f"{model_id}")
    
    def validate_model_files(self, model_id: str) -> dict:
        """
        验证模型文件的完整性
        
        Args:
            model_id: 模型ID
            
        Returns:
            验证结果字典
        """
        model_dir = os.path.join(self._model_dir, f"{model_id}")
        result = {
            "valid": False,
            "format": None,
            "config_file": None,
            "weight_file": None,
            "available_files": {},
            "errors": []
        }
        
        try:
            # 检测格式
            format_type, config_file, weight_file = self._detect_local_model_format(model_dir)
            result["format"] = format_type
            result["config_file"] = config_file
            result["weight_file"] = weight_file
            
            # 获取可用文件
            result["available_files"] = get_available_weight_files(model_dir)
            
            if format_type == "iotdb":
                # 验证IoTDB格式
                config_path = os.path.join(model_dir, config_file)
                weight_path = os.path.join(model_dir, weight_file)
                
                # 验证配置文件
                config_dict = parse_config_file(config_path)
                if not validate_iotdb_config(config_dict):
                    result["errors"].append("配置文件验证失败")
                
                # 验证权重文件
                try:
                    weights = load_weights_for_from_pretrained(model_dir, weight_file)
                    logger.debug(f"权重文件包含 {len(weights)} 个参数")
                    result["valid"] = True
                except Exception as e:
                    result["errors"].append(f"权重文件验证失败: {e}")
                    
            elif format_type == "legacy":
                result["valid"] = True
            else:
                result["errors"].append("未识别的模型格式")
                
        except Exception as e:
            result["errors"].append(f"验证过程出错: {e}")
            
        return result
    
    def _load_iotdb_model_with_from_pretrained(self, model_dir: str, config_file: str, weight_file: str, acceleration: bool) -> Callable:
        """
        完整的from_pretrained实现，支持IoTDB模型格式
        """
        config_path = os.path.join(model_dir, config_file)
        weight_path = os.path.join(model_dir, weight_file)
        
        # 使用复合键进行缓存
        cache_key = f"iotdb:{config_path}:{weight_path}"
        
        if cache_key in self._model_cache:
            model = self._model_cache[cache_key]
            if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
                return model
            else:
                model = torch.compile(model)
                self._model_cache[cache_key] = model
                return model
        
        try:
            # 解析配置文件
            config_dict = parse_config_file(config_path)
            model_type = config_dict.get("model_type", "unknown")
            
            logger.info(f"Loading IoTDB model with from_pretrained: {model_type}")
            
            # 动态导入模型类
            if model_type == "timer":
                from ainode.core.model.timerxl.modeling_timer import TimerForPrediction as ModelClass
                from ainode.core.model.timerxl.configuration_timer import TimerConfig as ConfigClass
            elif model_type == "sundial":
                from ainode.core.model.sundial.modeling_sundial import SundialForPrediction as ModelClass
                from ainode.core.model.sundial.configuration_sundial import SundialConfig as ConfigClass
            else:
                raise ValueError(f"Unsupported model type: {model_type}")
            
            # 创建配置对象
            model_config = ConfigClass.from_dict(config_dict)
            
            # 加载权重
            state_dict = load_weights_for_from_pretrained(model_dir, weight_file)
            
            # 使用from_pretrained创建模型
            model = ModelClass.from_pretrained(
                pretrained_model_name_or_path=model_dir,
                config=model_config,
                state_dict=state_dict,
                torch_dtype=torch.float32
            )
            
            model.eval()
            logger.info(f"Successfully loaded model {model_type} with from_pretrained")
            
            # 可选的TorchScript转换
            if self._should_convert_to_torchscript(config_dict):
                try:
                    input_length = config_dict.get("input_token_len", 96)
                    example_input = torch.randn(1, input_length)
                    traced_model = torch.jit.trace(model, example_input)
                    logger.debug(f"Converted to TorchScript: {model_type}")
                    model = traced_model
                except Exception as e:
                    logger.warning(f"TorchScript conversion failed: {e}")
            
            # 应用加速
            if acceleration:
                try:
                    model = torch.compile(model)
                    logger.debug(f"Applied acceleration: {model_type}")
                except Exception as e:
                    logger.warning(f"Acceleration failed: {e}")
            
            # 缓存模型
            self._model_cache[cache_key] = model
            return model
            
        except Exception as e:
            logger.error(f"from_pretrained failed for {model_type}: {e}")
            raise ModelLoadingError(model_type, str(e))

    def save_model_with_save_pretrained(self, model_id: str, model_obj, save_directory: str = None) -> bool:
        """
        使用save_pretrained保存模型
        """
        try:
            if save_directory is None:
                save_directory = os.path.join(self._model_dir, f"{model_id}_saved")
            
            # 确保保存目录存在
            os.makedirs(save_directory, exist_ok=True)
            
            # 检查模型是否支持save_pretrained
            if not hasattr(model_obj, 'save_pretrained'):
                logger.warning(f"Model {model_id} does not support save_pretrained")
                return False
            
            logger.info(f"Saving model {model_id} to {save_directory}")
            
            # 调用模型的save_pretrained方法
            model_obj.save_pretrained(
                save_directory=save_directory,
                safe_serialization=True,  # 使用safetensors格式
                save_config=True
            )
            
            # 验证保存的文件
            config_file = os.path.join(save_directory, "config.json")
            weight_file = os.path.join(save_directory, "model.safetensors")
            
            if os.path.exists(config_file) and os.path.exists(weight_file):
                logger.info(f"Model {model_id} saved successfully")
                return True
            else:
                logger.error(f"Save verification failed for {model_id}")
                return False
                
        except Exception as e:
            logger.error(f"save_pretrained failed for {model_id}: {e}")
            return False

    def _should_convert_to_torchscript(self, config_dict: dict) -> bool:
        """
        判断是否应该转换为TorchScript
        """
        # 可配置的转换策略
        model_type = config_dict.get("model_type", "")
        
        # 某些模型类型可能不适合TorchScript
        unsupported_types = ["sundial"]  # Sundial的扩散模型可能不适合TorchScript
        
        return model_type not in unsupported_types

    def clone_model_with_save_load(self, source_model_id: str, target_model_id: str) -> bool:
        """
        使用save_pretrained和from_pretrained克隆模型
        """
        try:
            # 加载源模型
            source_model = self.load_model(source_model_id, acceleration=False)
            
            # 保存到临时目录
            temp_dir = os.path.join(self._model_dir, f"temp_{target_model_id}")
            if self.save_model_with_save_pretrained(source_model_id, source_model, temp_dir):
                
                # 重新加载到目标位置
                target_dir = os.path.join(self._model_dir, target_model_id)
                shutil.move(temp_dir, target_dir)
                
                logger.info(f"Model cloned: {source_model_id} -> {target_model_id}")
                return True
            else:
                # 清理临时目录
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                return False
                
        except Exception as e:
            logger.error(f"Model cloning failed: {e}")
            return False
