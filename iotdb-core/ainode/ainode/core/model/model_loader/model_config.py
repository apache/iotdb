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
"""
thuTL 模型配置管理，支持 HuggingFace 格式和多版本兼容
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass
class ModelConfig:
    """thuTL 统一模型配置基类"""

    # 基础字段
    model_type: str
    hidden_size: int
    num_hidden_layers: int
    num_attention_heads: int
    input_token_len: int
    output_token_lens: List[int]

    # 通用字段
    hidden_act: str = "silu"
    intermediate_size: int = 2048
    max_position_embeddings: int = 10000
    rope_theta: int = 10000
    torch_dtype: str = "float32"
    initializer_range: float = 0.02
    use_cache: bool = True

    # 版本信息
    transformers_version: Optional[str] = None
    thuTL_version: Optional[str] = "1.0.0"

    # 未知字段存储
    _extras: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, config_path: Union[str, Path]) -> "ModelConfig":
        """从 JSON 配置文件加载"""
        config_data = json.loads(Path(config_path).read_text(encoding="utf-8"))

        # 应用版本兼容性补丁
        config_data = cls._apply_version_patches(config_data)

        # 根据模型类型选择具体配置类
        model_type = config_data.get("model_type", "unknown")
        config_cls = cls._get_config_class(model_type)

        # 分离已知和未知字段
        known_fields = set()
        for cls_in_mro in config_cls.__mro__:
            if hasattr(cls_in_mro, "__dataclass_fields__"):
                known_fields.update(cls_in_mro.__dataclass_fields__.keys())

        known = {}
        extras = {}
        for k, v in config_data.items():
            if k in known_fields and not k.startswith("_"):
                known[k] = v
            else:
                extras[k] = v

        known["_extras"] = extras
        return config_cls(**known)

    @classmethod
    def _get_config_class(cls, model_type: str) -> type:
        """根据模型类型选择配置类"""
        if model_type == "timer":
            from ..timerxl.configuration_timer import TimerConfig

            return TimerConfig
        elif model_type == "sundial":
            from ..sundial.configuration_sundial import SundialConfig

            return SundialConfig
        else:
            return cls  # 使用基类作为后备

    @classmethod
    def _apply_version_patches(cls, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """应用版本兼容性补丁"""
        version = config_data.get("transformers_version", "unknown")

        # 补丁示例：不同版本间的字段映射
        patches = {
            # 历史版本兼容
            "legacy": {
                "n_embd": "hidden_size",
                "n_layer": "num_hidden_layers",
                "n_head": "num_attention_heads",
                "seq_len": "input_token_len",
            },
            # 特定版本补丁
            "4.30.0": {"attention_dropout": "attn_dropout_rate"},
        }

        # 应用通用legacy补丁
        if "legacy" in patches:
            for old_key, new_key in patches["legacy"].items():
                if old_key in config_data and new_key not in config_data:
                    config_data[new_key] = config_data.pop(old_key)

        # 应用版本特定补丁
        if version in patches:
            for old_key, new_key in patches[version].items():
                if old_key in config_data and new_key not in config_data:
                    config_data[new_key] = config_data.pop(old_key)

        return config_data

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {}
        for field_name, field_def in self.__dataclass_fields__.items():
            if not field_name.startswith("_"):
                result[field_name] = getattr(self, field_name)

        # 添加扩展字段
        result.update(self._extras)
        return result

    def save_json(self, path: Union[str, Path]):
        """保存为 JSON 文件"""
        Path(path).write_text(
            json.dumps(self.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
        )


# 具体模型配置类将在各自的模块中定义
