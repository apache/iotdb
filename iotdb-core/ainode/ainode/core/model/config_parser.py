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
from pathlib import Path
from typing import Any, Dict, Union

import yaml

from ainode.core.log import Logger

logger = Logger()


def parse_config_file(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    解析配置文件，支持JSON和YAML格式

    Args:
        config_path: 配置文件路径

    Returns:
        配置字典
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    suffix = config_path.suffix.lower()

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            if suffix == ".json":
                return json.load(f)
            elif suffix in [".yaml", ".yml"]:
                return yaml.safe_load(f)
            else:
                # 尝试JSON解析
                content = f.read()
                try:
                    return json.loads(content)
                except json.JSONDecodeError:
                    # 尝试YAML解析
                    return yaml.safe_load(content)
    except Exception as e:
        logger.error(f"解析配置文件失败: {config_path}, 错误: {e}")
        raise


def convert_iotdb_config_to_ainode_format(
    iotdb_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    将IoTDB配置转换为AINode格式 (原thuTL_config函数重命名)

    Args:
        iotdb_config: IoTDB配置字典

    Returns:
        AINode格式的配置字典
    """
    # 提取基础信息
    model_type = iotdb_config.get("model_type", "unknown")
    input_length = iotdb_config.get("input_token_len", 96)
    output_length = iotdb_config.get("output_token_lens", [96])[0] if iotdb_config.get("output_token_lens") else 96

    # 转换为AINode格式
    ainode_config = {
        "configs": {
            "input_shape": [input_length, 1],  # IoTDB时序模型输入为单维
            "output_shape": [output_length, 1],  # IoTDB时序模型输出为单维
            "input_type": ["float32"],
            "output_type": ["float32"],
        },
        "attributes": {
            "model_type": model_type,
            "iotdb_model": True,
            "original_config": iotdb_config,
        },
    }

    logger.debug(f"转换IoTDB配置: {model_type} -> AINode格式")
    return ainode_config


# 保持向后兼容的别名
def convert_thuTL_config_to_ainode_format(thuTL_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    向后兼容的函数别名
    """
    logger.warning("convert_thuTL_config_to_ainode_format is deprecated, use convert_iotdb_config_to_ainode_format instead")
    return convert_iotdb_config_to_ainode_format(thuTL_config)


def validate_iotdb_config(config: Dict[str, Any]) -> bool:
    """
    验证IoTDB配置的有效性
    
    Args:
        config: IoTDB配置字典
        
    Returns:
        是否有效
    """
    required_fields = ["model_type", "input_token_len", "output_token_lens"]
    
    for field in required_fields:
        if field not in config:
            logger.error(f"缺少必需字段: {field}")
            return False
    
    model_type = config.get("model_type")
    supported_types = ["timer", "sundial"]
    if model_type not in supported_types:
        logger.error(f"不支持的模型类型: {model_type}, 支持的类型: {supported_types}")
        return False
    
    # 验证输入输出长度
    input_len = config.get("input_token_len")
    output_lens = config.get("output_token_lens")
    
    if not isinstance(input_len, int) or input_len <= 0:
        logger.error(f"无效的输入长度: {input_len}")
        return False
        
    if not isinstance(output_lens, list) or len(output_lens) == 0:
        logger.error(f"无效的输出长度配置: {output_lens}")
        return False
    
    logger.info(f"IoTDB模型配置验证通过: {model_type}")
    return True


def apply_config_patches(config: Dict[str, Any], model_type: str) -> Dict[str, Any]:
    """
    应用配置补丁，支持版本兼容

    Args:
        config: 原始配置
        model_type: 模型类型

    Returns:
        应用补丁后的配置
    """
    patches = {
        "timer": {
            # TimerXL特定补丁
            "n_embd": "hidden_size",
            "n_layer": "num_hidden_layers",
            "n_head": "num_attention_heads",
            "seq_len": "input_token_len",
        },
        "sundial": {
            # Sundial特定补丁
            "diff_steps": "num_sampling_steps",
            "flow_depth": "flow_loss_depth",
        },
        # 添加更多模型类型的补丁支持
        "timerxl": {
            # TimerXL的别名支持
            "n_embd": "hidden_size",
            "n_layer": "num_hidden_layers", 
            "n_head": "num_attention_heads",
            "seq_len": "input_token_len",
        }
    }

    if model_type in patches:
        for old_key, new_key in patches[model_type].items():
            if old_key in config and new_key not in config:
                config[new_key] = config.pop(old_key)
                logger.debug(f"应用补丁: {old_key} -> {new_key}")

    return config


def detect_config_format(config_path: Union[str, Path]) -> str:
    """
    检测配置文件格式
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置格式: "iotdb" 或 "legacy"
    """
    config_path = Path(config_path)
    suffix = config_path.suffix.lower()
    
    if suffix == ".json":
        try:
            config = parse_config_file(config_path)
            if "model_type" in config and config.get("model_type") in ["timer", "sundial"]:
                return "iotdb"
        except:
            pass
    elif suffix in [".yaml", ".yml"]:
        try:
            config = parse_config_file(config_path)
            if "configs" in config and "attributes" in config:
                return "legacy"
        except:
            pass
    
    return "unknown"

def create_model_config_for_save_pretrained(model_type: str, model_params: dict) -> dict:
    """
    为save_pretrained创建标准配置格式
    """
    base_config = {
        "model_type": model_type,
        "torch_dtype": "float32",
        "transformers_version": "4.40.1",
    }
    
    if model_type == "timer":
        config = {
            **base_config,
            "input_token_len": model_params.get("input_token_len", 96),
            "output_token_lens": model_params.get("output_token_lens", [96]),
            "hidden_size": model_params.get("hidden_size", 1024),
            "intermediate_size": model_params.get("intermediate_size", 2048),
            "num_hidden_layers": model_params.get("num_hidden_layers", 8),
            "num_attention_heads": model_params.get("num_attention_heads", 8),
            "hidden_act": model_params.get("hidden_act", "silu"),
            "use_cache": model_params.get("use_cache", True),
            "rope_theta": model_params.get("rope_theta", 10000),
            "attention_dropout": model_params.get("attention_dropout", 0.0),
            "initializer_range": model_params.get("initializer_range", 0.02),
            "max_position_embeddings": model_params.get("max_position_embeddings", 10000),
        }
    elif model_type == "sundial":
        config = {
            **base_config,
            "input_token_len": model_params.get("input_token_len", 16),
            "output_token_lens": model_params.get("output_token_lens", [720]),
            "hidden_size": model_params.get("hidden_size", 768),
            "intermediate_size": model_params.get("intermediate_size", 3072),
            "num_hidden_layers": model_params.get("num_hidden_layers", 12),
            "num_attention_heads": model_params.get("num_attention_heads", 12),
            "hidden_act": model_params.get("hidden_act", "silu"),
            "use_cache": model_params.get("use_cache", True),
            "rope_theta": model_params.get("rope_theta", 10000),
            "dropout_rate": model_params.get("dropout_rate", 0.1),
            "initializer_range": model_params.get("initializer_range", 0.02),
            "max_position_embeddings": model_params.get("max_position_embeddings", 10000),
            "flow_loss_depth": model_params.get("flow_loss_depth", 3),
            "num_sampling_steps": model_params.get("num_sampling_steps", 50),
            "diffusion_batch_mul": model_params.get("diffusion_batch_mul", 4),
        }
    else:
        raise ValueError(f"Unsupported model type for config creation: {model_type}")
    
    return config

def extract_model_params_from_config(config_path: str) -> dict:
    """
    从配置文件提取模型参数用于from_pretrained
    """
    config_dict = parse_config_file(config_path)
    
    # 标准化参数名称
    param_mapping = {
        "seq_len": "input_token_len",
        "n_embd": "hidden_size",
        "n_layer": "num_hidden_layers",
        "n_head": "num_attention_heads",
        "diff_steps": "num_sampling_steps",
        "flow_depth": "flow_loss_depth",
    }
    
    # 应用参数映射
    for old_key, new_key in param_mapping.items():
        if old_key in config_dict and new_key not in config_dict:
            config_dict[new_key] = config_dict.pop(old_key)
    
    return config_dict