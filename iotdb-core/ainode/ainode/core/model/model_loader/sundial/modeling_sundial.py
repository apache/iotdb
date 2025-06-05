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
import math
from typing import Dict, Optional

import torch
import torch.nn as nn

from ..base_model import ThuTLBaseModel
from .attention import SundialAttention
from .configuration import SundialConfig


class DiffusionTimeStep(nn.Module):
    """扩散时间步嵌入"""

    def __init__(self, hidden_size: int):
        super().__init__()
        self.hidden_size = hidden_size

    def forward(self, timesteps: torch.Tensor):
        """
        生成时间步嵌入

        Args:
            timesteps: [batch_size] 时间步张量

        Returns:
            [batch_size, hidden_size] 时间步嵌入
        """
        # 时间步编码（类似Transformer的位置编码）
        half_dim = self.hidden_size // 2
        emb = math.log(10000) / (half_dim - 1)
        emb = torch.exp(torch.arange(half_dim, device=timesteps.device) * -emb)
        emb = timesteps[:, None] * emb[None, :]
        emb = torch.cat([torch.sin(emb), torch.cos(emb)], dim=-1)
        return emb


class FlowLayer(nn.Module):
    """Flow变换层"""

    def __init__(self, config: SundialConfig):
        super().__init__()
        self.hidden_size = config.hidden_size
        self.flow_depth = config.flow_loss_depth

        # Flow变换网络
        self.flow_net = nn.Sequential(
            nn.Linear(self.hidden_size, self.hidden_size * 2),
            nn.SiLU(),
            nn.Linear(self.hidden_size * 2, self.hidden_size),
            nn.Dropout(config.dropout_rate),
        )

    def forward(self, x: torch.Tensor, reverse: bool = False):
        """
        Flow变换

        Args:
            x: [batch_size, seq_len, hidden_size] 输入张量
            reverse: 是否执行逆变换

        Returns:
            变换后的张量
        """
        if reverse:
            # 逆Flow变换
            return x - self.flow_net(x)
        else:
            # 正Flow变换
            return x + self.flow_net(x)


class SundialBlock(nn.Module):
    """Sundial Transformer Block with Diffusion-Flow"""

    def __init__(self, config: SundialConfig):
        super().__init__()
        self.config = config

        # 使用Sundial专用的注意力
        self.attention = SundialAttention(config)

        # Flow层
        self.flow_layer = FlowLayer(config)

        # 标准FFN
        self.feed_forward = nn.Sequential(
            nn.Linear(config.hidden_size, config.intermediate_size),
            nn.SiLU(),
            nn.Linear(config.intermediate_size, config.hidden_size),
            nn.Dropout(config.dropout_rate),
        )

        # 层归一化
        self.ln1 = nn.LayerNorm(config.hidden_size)
        self.ln2 = nn.LayerNorm(config.hidden_size)
        self.ln3 = nn.LayerNorm(config.hidden_size)

    def forward(self, x: torch.Tensor, timestep_emb: Optional[torch.Tensor] = None):
        """
        前向传播

        Args:
            x: [batch_size, seq_len, hidden_size] 输入张量
            timestep_emb: [batch_size, hidden_size] 时间步嵌入（可选）

        Returns:
            [batch_size, seq_len, hidden_size] 输出张量
        """
        # 时间注意力
        attn_out = self.attention(self.ln1(x))
        x = x + attn_out

        # Flow变换（融合时间步信息）
        if timestep_emb is not None:
            # 将时间步嵌入广播到序列维度
            x = x + timestep_emb.unsqueeze(
                1
            )  # [batch_size, 1, hidden_size] -> [batch_size, seq_len, hidden_size]

        flow_out = self.flow_layer(self.ln2(x))
        x = x + flow_out

        # 前馈网络
        ff_out = self.feed_forward(self.ln3(x))
        x = x + ff_out

        return x


@SequenceModel.register("sundial")
class Sundial(SequenceModel):
    """
    Sundial模型：基于Diffusion-Flow的时序预测模型

    特点：
    - 扩散采样过程
    - Flow变换层
    - 时间步嵌入
    """

    config_class = SundialConfig  # 指定配置类

    def __init__(self, config: SundialConfig):
        super().__init__(config)

    def build_layers(self):
        """构建模型层"""
        # 输入嵌入
        self.input_embedding = nn.Linear(1, self.config.hidden_size)

        # 扩散时间步嵌入
        self.timestep_embedding = DiffusionTimeStep(self.config.hidden_size)

        # Transformer层
        self.layers = nn.ModuleList(
            [SundialBlock(self.config) for _ in range(self.config.num_hidden_layers)]
        )

        # 输出层
        self.output_norm = nn.LayerNorm(self.config.hidden_size)
        self.output_head = nn.Linear(
            self.config.hidden_size, self.config.output_token_lens[0]
        )

        # 权重初始化
        self.apply(self._init_weights)

    def _init_weights(self, module):
        """权重初始化"""
        if isinstance(module, nn.Linear):
            torch.nn.init.normal_(
                module.weight, mean=0.0, std=self.config.initializer_range
            )
            if module.bias is not None:
                torch.nn.init.zeros_(module.bias)

    def forward(self, input_ids: torch.Tensor, **kwargs) -> torch.Tensor:
        """
        前向传播

        Args:
            input_ids: [batch_size, seq_len] 输入序列

        Returns:
            [batch_size, output_len] 预测结果
        """
        batch_size, seq_len = input_ids.shape

        # 输入嵌入: [batch_size, seq_len] -> [batch_size, seq_len, hidden_size]
        x = self.input_embedding(input_ids.unsqueeze(-1))

        # 扩散采样循环
        for step in range(self.config.num_sampling_steps):
            # 生成当前步的时间步嵌入
            timesteps = torch.full(
                (batch_size,), step, device=input_ids.device, dtype=torch.long
            )
            timestep_emb = self.timestep_embedding(timesteps)

            # 通过所有Transformer层
            for layer in self.layers:
                x = layer(x, timestep_emb)

        # 输出处理
        x = self.output_norm(x)

        # 使用最后一个时间步的表示进行预测
        last_hidden = x[:, -1, :]  # [batch_size, hidden_size]
        predictions = self.output_head(last_hidden)  # [batch_size, output_len]

        return predictions

    def sample_with_steps(
        self, input_ids: torch.Tensor, num_steps: Optional[int] = None
    ) -> torch.Tensor:
        """
        使用指定步数进行采样

        Args:
            input_ids: 输入序列
            num_steps: 采样步数，默认使用配置中的值

        Returns:
            预测结果
        """
        if num_steps is None:
            num_steps = self.config.num_sampling_steps

        batch_size, seq_len = input_ids.shape
        x = self.input_embedding(input_ids.unsqueeze(-1))

        # 自定义步数的扩散采样
        for step in range(num_steps):
            timesteps = torch.full(
                (batch_size,), step, device=input_ids.device, dtype=torch.long
            )
            timestep_emb = self.timestep_embedding(timesteps)

            for layer in self.layers:
                x = layer(x, timestep_emb)

        x = self.output_norm(x)
        last_hidden = x[:, -1, :]
        predictions = self.output_head(last_hidden)

        return predictions

    def get_intermediate_representations(
        self, input_ids: torch.Tensor
    ) -> Dict[str, torch.Tensor]:
        """
        获取中间表示，用于分析和调试

        Args:
            input_ids: 输入序列

        Returns:
            包含中间表示的字典
        """
        batch_size, seq_len = input_ids.shape
        x = self.input_embedding(input_ids.unsqueeze(-1))

        representations = {
            "input_embedding": x.clone(),
            "layer_outputs": [],
            "timestep_embeddings": [],
        }

        for step in range(self.config.num_sampling_steps):
            timesteps = torch.full(
                (batch_size,), step, device=input_ids.device, dtype=torch.long
            )
            timestep_emb = self.timestep_embedding(timesteps)
            representations["timestep_embeddings"].append(timestep_emb.clone())

            step_layer_outputs = []
            for i, layer in enumerate(self.layers):
                x = layer(x, timestep_emb)
                step_layer_outputs.append(x.clone())

            representations["layer_outputs"].append(step_layer_outputs)

        representations["final_output"] = self.output_norm(x)
        return representations
