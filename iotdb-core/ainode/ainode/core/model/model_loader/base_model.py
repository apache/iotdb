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
thuTL 基础模型抽象，适配 AINode 架构
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Iterator, Tuple, Union

import torch
import torch.nn as nn
from model_config import ModelConfig


class BaseModel(ABC, nn.Module):
    """thuTL 模型基类，适配 AINode 架构"""

    def __init__(self, config: "ModelConfig"):
        super().__init__()
        self.config = config
        self.build_layers()

    @abstractmethod
    def build_layers(self):
        """构建模型层，子类必须实现"""
        pass

    @abstractmethod
    def forward(self, input_ids: torch.Tensor, **kwargs) -> torch.Tensor:
        """前向传播，子类必须实现"""
        pass

    def preprocess(self, data: Union[torch.Tensor, Any]) -> torch.Tensor:
        """预处理输入数据"""
        if not isinstance(data, torch.Tensor):
            data = torch.tensor(data, dtype=torch.float32)

        if data.dim() == 1:
            data = data.unsqueeze(0)

        # 截断或填充到指定长度
        target_len = self.config.input_token_len
        if data.size(-1) > target_len:
            data = data[..., :target_len]
        elif data.size(-1) < target_len:
            pad_size = target_len - data.size(-1)
            data = torch.nn.functional.pad(data, (0, pad_size))

        return data

    def postprocess(self, logits: torch.Tensor) -> torch.Tensor:
        """后处理输出"""
        target_len = self.config.output_token_lens[0]
        return logits[..., :target_len]

    @classmethod
    def from_pretrained(
        cls, config_path: Union[str, Path], weights_path: Union[str, Path], **kwargs
    ) -> "BaseModel":
        """从预训练模型加载"""
        from .model_config import ModelConfig
        from .weight_loader import load_weights

        # 加载配置
        config = ModelConfig.from_json(config_path)

        # 创建模型实例
        model = cls(config, **kwargs)

        # 加载权重
        weights = load_weights(weights_path)
        missing_keys, unexpected_keys = model.load_state_dict(weights, strict=False)

        if missing_keys:
            print(f"Missing keys: {missing_keys}")
        if unexpected_keys:
            print(f"Unexpected keys: {unexpected_keys}")

        return model


# 模型注册机制
_MODEL_REGISTRY: Dict[str, type] = {}


def register_model(name: str):
    """模型注册装饰器"""

    def wrapper(cls):
        _MODEL_REGISTRY[name] = cls
        return cls

    return wrapper


def get_model_class(name: str) -> type:
    """获取已注册的模型类"""
    if name not in _MODEL_REGISTRY:
        raise ValueError(f"Unknown model type: {name}")
    return _MODEL_REGISTRY[name]


def list_available_models() -> list:
    """列出所有可用模型"""
    return list(_MODEL_REGISTRY.keys())
