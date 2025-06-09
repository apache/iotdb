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

import json
import os
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
from urllib.parse import urlparse

from ainode.core.constant import DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
from ainode.core.exception import InvalidUriError, ModelFormatError
from ainode.core.log import Logger

logger = Logger()

# IoTDB模型文件名常量
IOTDB_CONFIG_FILES = ["config.json", "configuration.json"]  
IOTDB_WEIGHT_FILES = ["model.safetensors", "pytorch_model.safetensors", "model.pt", "pytorch_model.pt"]

def detect_model_format(model_path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    检测模型格式：legacy (model.pt + config.yaml) 或 IoTDB (config.json + safetensors)
    
    Args:
        model_path: 模型目录路径
        
    Returns:
        (format_type, config_file, weight_file): 格式类型和对应的文件名
    """
    model_dir = Path(model_path)
    
    if not model_dir.exists():
        logger.error(f"模型路径不存在: {model_path}")
        return None, None, None
    
    # 检查 IoTDB 格式
    for config_file in IOTDB_CONFIG_FILES:
        config_path = model_dir / config_file
        if config_path.exists():
            # 查找权重文件
            for weight_file in IOTDB_WEIGHT_FILES:
                weight_path = model_dir / weight_file
                if weight_path.exists():
                    logger.info(f"检测到 IoTDB 格式: {config_file} + {weight_file}")
                    return "iotdb", config_file, weight_file
    
    # 检查 legacy 格式
    legacy_config = model_dir / DEFAULT_CONFIG_FILE_NAME
    legacy_model = model_dir / DEFAULT_MODEL_FILE_NAME
    if legacy_config.exists() and legacy_model.exists():
        logger.info(f"检测到 legacy 格式: {DEFAULT_CONFIG_FILE_NAME} + {DEFAULT_MODEL_FILE_NAME}")
        return "legacy", DEFAULT_CONFIG_FILE_NAME, DEFAULT_MODEL_FILE_NAME
    
    logger.warning(f"未能识别模型格式: {model_path}")
    return None, None, None

def validate_iotdb_model_config(config_dict: Dict[str, Any]) -> bool:
    """
    验证IoTDB模型配置的有效性
    
    Args:
        config_dict: 配置字典
        
    Returns:
        是否有效
    """
    required_fields = ["model_type", "input_token_len", "output_token_lens"]
    
    for field in required_fields:
        if field not in config_dict:
            logger.error(f"缺少必需字段: {field}")
            return False
    
    model_type = config_dict.get("model_type")
    supported_types = ["timer", "sundial"]
    if model_type not in supported_types:
        logger.error(f"不支持的模型类型: {model_type}, 支持的类型: {supported_types}")
        return False
    
    # 验证输入输出长度
    input_len = config_dict.get("input_token_len")
    output_lens = config_dict.get("output_token_lens")
    
    if not isinstance(input_len, int) or input_len <= 0:
        logger.error(f"无效的输入长度: {input_len}")
        return False
        
    if not isinstance(output_lens, list) or len(output_lens) == 0:
        logger.error(f"无效的输出长度配置: {output_lens}")
        return False
    
    logger.info(f"IoTDB模型配置验证通过: {model_type}")
    return True

def parse_model_uri(uri: str) -> Tuple[bool, str]:
    """
    解析模型URI，判断是网络路径还是本地路径
    
    Args:
        uri: 模型URI
        
    Returns:
        (is_network, parsed_uri): 是否为网络路径和解析后的URI
    """
    try:
        parsed = urlparse(uri)
        is_network = parsed.scheme in ("http", "https")
        
        if is_network:
            logger.info(f"检测到网络URI: {uri}")
            return True, uri
        else:
            # 处理本地路径
            if parsed.scheme == "file":
                uri = uri[7:]  # 移除 file://
            
            # 处理 ~ 符号
            uri = os.path.expanduser(uri)
            logger.info(f"检测到本地URI: {uri}")
            return False, uri
            
    except Exception as e:
        logger.error(f"URI解析失败: {uri}, 错误: {e}")
        raise InvalidUriError(uri)

def get_model_info_from_config(config_path: str) -> Dict[str, Any]:
    """
    从配置文件中提取模型信息
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        模型信息字典
    """
    try:
        config_file = Path(config_path)
        
        if config_file.suffix.lower() == '.json':
            # IoTDB格式配置
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            model_info = {
                "format": "iotdb",
                "model_type": config.get("model_type", "unknown"),
                "input_length": config.get("input_token_len", 0),
                "output_length": config.get("output_token_lens", [0])[0] if config.get("output_token_lens") else 0,
                "architecture": config.get("architecture", ""),
                "task": config.get("task", "forecasting")
            }
            
        else:
            # Legacy格式配置  
            import yaml
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                
            configs = config.get("configs", {})
            model_info = {
                "format": "legacy",
                "model_type": "legacy",
                "input_length": configs.get("input_shape", [0, 0])[0],
                "output_length": configs.get("output_shape", [0, 0])[0],
                "architecture": "legacy",
                "task": "forecasting"
            }
            
        logger.info(f"提取模型信息: {model_info}")
        return model_info
        
    except Exception as e:
        logger.error(f"提取模型信息失败: {config_path}, 错误: {e}")
        raise ModelFormatError(config_path, "valid config file")

def validate_model_name(model_name: str) -> bool:
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
        logger.debug(f"模型名称验证通过: {model_name}")
        return True
    else:
        logger.error(f"无效的模型名称: {model_name}")
        return False

def create_model_status_message(status: str, details: str = "") -> str:
    """
    创建模型状态消息
    
    Args:
        status: 状态
        details: 详细信息
        
    Returns:
        状态消息
    """
    if details:
        return f"模型状态: {status} - {details}"
    else:
        return f"模型状态: {status}"