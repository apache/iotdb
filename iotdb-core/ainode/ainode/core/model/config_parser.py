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
    将 IoTDB 配置转换为 AINode 格式

    Args:
        iotdb_config: IoTDB 配置字典

    Returns:
        AINode 格式的配置字典
    """
    # 提取基础信息
    model_type = iotdb_config.get("model_type", "unknown")
    input_length = iotdb_config.get("input_token_len", 96)
    output_length = iotdb_config.get("output_token_lens", [96])[0]

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

    logger.debug(f"转换 IoTDB 配置: {model_type} -> AINode格式")
    return ainode_config


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
    }

    if model_type in patches:
        for old_key, new_key in patches[model_type].items():
            if old_key in config and new_key not in config:
                config[new_key] = config.pop(old_key)
                logger.debug(f"应用补丁: {old_key} -> {new_key}")

    return config