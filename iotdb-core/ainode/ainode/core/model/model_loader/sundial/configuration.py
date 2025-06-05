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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class SundialConfig:
    """Sundial 模型配置"""

    # 模型标识
    model_type: str = "sundial"

    # 核心架构参数
    hidden_size: int = 768
    num_hidden_layers: int = 12
    num_attention_heads: int = 12

    # 时序相关参数
    input_token_len: int = 16
    output_token_lens: List[int] = field(default_factory=lambda: [720])

    # Sundial专用参数
    diffusion_batch_mul: int = 4
    flow_loss_depth: int = 3
    num_sampling_steps: int = 50
    dropout_rate: float = 0.1

    # 注意力参数
    rope_theta: int = 10000

    # 前馈网络参数
    intermediate_size: int = 2048
    hidden_act: str = "silu"

    # 训练参数
    initializer_range: float = 0.02
    max_position_embeddings: int = 10000
    torch_dtype: str = "float32"
    use_cache: bool = True

    # 扩展字段，支持未来参数
    _extras: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, path: str) -> "SundialConfig":
        """从JSON文件加载配置"""
        config_data = json.loads(Path(path).read_text())

        # 获取已声明的字段
        declared_fields = set(cls.__dataclass_fields__.keys())

        # 分离已知和未知字段
        known = {}
        extras = {}
        for k, v in config_data.items():
            if k in declared_fields and not k.startswith("_"):
                known[k] = v
            else:
                extras[k] = v

        known["_extras"] = extras
        return cls(**known)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        result = {}
        for field_name, field_def in self.__dataclass_fields__.items():
            if not field_name.startswith("_"):
                result[field_name] = getattr(self, field_name)

        # 添加扩展字段
        result.update(self._extras)
        return result

    def save_json(self, path: str):
        """保存为JSON文件"""
        Path(path).write_text(json.dumps(self.to_dict(), indent=2, ensure_ascii=False))

    def get_diffusion_config(self) -> Dict[str, Any]:
        """获取扩散相关配置"""
        return {
            "diffusion_batch_mul": self.diffusion_batch_mul,
            "num_sampling_steps": self.num_sampling_steps,
            "dropout_rate": self.dropout_rate,
        }

    def get_flow_config(self) -> Dict[str, Any]:
        """获取Flow相关配置"""
        return {
            "flow_loss_depth": self.flow_loss_depth,
            "hidden_size": self.hidden_size,
            "dropout_rate": self.dropout_rate,
        }
