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
from typing import List, Optional

import torch
import torch.nn as nn


class SundialRoPEEmbedding(nn.Module):
    """Sundial专用的旋转位置编码"""

    def __init__(self, dim: int, max_seq_len: int = 10000, theta: float = 10000.0):
        super().__init__()
        self.dim = dim
        self.theta = theta

        if dim % 2 != 0:
            raise ValueError(f"RoPE dimension must be even, got {dim}")

        # 生成频率，只需要 dim//2 个频率
        inv_freq = 1.0 / (theta ** (torch.arange(0, dim, 2).float() / dim))
        self.register_buffer("inv_freq", inv_freq)

    def forward(self, x: torch.Tensor, seq_len: int):
        """
        应用旋转位置编码

        Args:
            x: [batch, seq_len, num_heads, head_dim] 或 [batch, num_heads, seq_len, head_dim]
            seq_len: 序列长度

        Returns:
            应用RoPE后的张量
        """
        device = x.device
        dtype = x.dtype

        # 生成位置索引
        seq_idx = torch.arange(seq_len, device=device, dtype=dtype)

        # 计算频率矩阵 [seq_len, dim//2]
        freqs = torch.outer(seq_idx, self.inv_freq)

        # 生成cos和sin [seq_len, dim//2]
        cos = freqs.cos()
        sin = freqs.sin()

        # 扩展到完整维度 [seq_len, dim]
        cos = torch.cat([cos, cos], dim=-1)
        sin = torch.cat([sin, sin], dim=-1)

        # 应用旋转变换
        return self._apply_rotary_pos_emb(x, cos, sin)

    def _apply_rotary_pos_emb(
        self, x: torch.Tensor, cos: torch.Tensor, sin: torch.Tensor
    ):
        """应用旋转位置嵌入"""
        # 自动检测张量格式
        if x.dim() == 4:
            if x.shape[1] == cos.shape[0]:  # [batch, seq_len, num_heads, head_dim]
                seq_dim = 1
                head_dim = x.shape[-1]
            else:  # [batch, num_heads, seq_len, head_dim]
                seq_dim = 2
                head_dim = x.shape[-1]
        else:
            raise ValueError(f"Expected 4D tensor, got {x.dim()}D")

        # 确保cos和sin的维度匹配
        target_shape = list(x.shape)
        cos_sin_shape = [1] * len(target_shape)
        cos_sin_shape[seq_dim] = cos.shape[0]  # seq_len
        cos_sin_shape[-1] = head_dim  # head_dim

        cos = cos[: target_shape[seq_dim], :head_dim].view(cos_sin_shape)
        sin = sin[: target_shape[seq_dim], :head_dim].view(cos_sin_shape)

        # 分离x为两部分进行旋转
        x1 = x[..., : head_dim // 2]
        x2 = x[..., head_dim // 2 :]

        # 应用旋转变换
        cos1 = cos[..., : head_dim // 2]
        cos2 = cos[..., head_dim // 2 :]
        sin1 = sin[..., : head_dim // 2]
        sin2 = sin[..., head_dim // 2 :]

        # RoPE变换：[x1*cos - x2*sin, x1*sin + x2*cos]
        rotated_x1 = x1 * cos1 - x2 * sin1
        rotated_x2 = x1 * sin2 + x2 * cos2

        return torch.cat([rotated_x1, rotated_x2], dim=-1)


class SundialAttention(nn.Module):
    """Sundial专用的时间感知注意力机制"""

    def __init__(self, config):
        super().__init__()
        self.hidden_size = config.hidden_size
        self.num_heads = config.num_attention_heads
        self.head_dim = self.hidden_size // self.num_heads
        self.dropout_rate = getattr(config, "dropout_rate", 0.0)

        assert (
            self.hidden_size % self.num_heads == 0
        ), f"hidden_size ({self.hidden_size}) must be divisible by num_heads ({self.num_heads})"

        # Q, K, V投影层
        self.q_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.k_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.v_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.o_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)

        # RoPE位置编码
        self.rope = SundialRoPEEmbedding(self.head_dim, theta=config.rope_theta)

        # Dropout
        self.dropout = nn.Dropout(self.dropout_rate)

        # 缩放因子
        self.scale = 1.0 / math.sqrt(self.head_dim)

    def forward(self, x: torch.Tensor, attention_mask: Optional[torch.Tensor] = None):
        """
        前向传播

        Args:
            x: [batch_size, seq_len, hidden_size] 输入张量
            attention_mask: [batch_size, seq_len, seq_len] 注意力掩码（可选）

        Returns:
            [batch_size, seq_len, hidden_size] 注意力输出
        """
        batch_size, seq_len, _ = x.shape

        # 投影到 Q, K, V
        q = self.q_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        k = self.k_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        v = self.v_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)

        # 应用RoPE位置编码
        q = self.rope(q, seq_len)
        k = self.rope(k, seq_len)

        # 重排维度到 [batch, num_heads, seq_len, head_dim]
        q = q.transpose(1, 2)
        k = k.transpose(1, 2)
        v = v.transpose(1, 2)

        # 计算注意力分数
        scores = torch.matmul(q, k.transpose(-2, -1)) * self.scale

        # 应用注意力掩码
        if attention_mask is not None:
            scores += attention_mask

        # Softmax归一化
        attn_weights = torch.softmax(scores, dim=-1)
        attn_weights = self.dropout(attn_weights)

        # 应用注意力权重
        attn_output = torch.matmul(attn_weights, v)

        # 重排维度并合并头
        attn_output = attn_output.transpose(1, 2).contiguous()
        attn_output = attn_output.view(batch_size, seq_len, self.hidden_size)

        # 输出投影
        return self.o_proj(attn_output)

    def compute_attention_weights(self, x: torch.Tensor) -> torch.Tensor:
        """
        计算并返回注意力权重（用于可视化）

        Args:
            x: 输入张量

        Returns:
            [batch_size, num_heads, seq_len, seq_len] 注意力权重
        """
        batch_size, seq_len, _ = x.shape

        q = self.q_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        k = self.k_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)

        q = self.rope(q, seq_len).transpose(1, 2)
        k = self.rope(k, seq_len).transpose(1, 2)

        scores = torch.matmul(q, k.transpose(-2, -1)) * self.scale
        attn_weights = torch.softmax(scores, dim=-1)

        return attn_weights


class MultiScaleAttention(nn.Module):
    """多尺度注意力机制（Sundial的扩展变体）"""

    def __init__(self, config, scales: List[int] = [1, 2, 4]):
        super().__init__()
        self.scales = scales
        self.attentions = nn.ModuleList([SundialAttention(config) for _ in scales])
        self.fusion = nn.Linear(len(scales) * config.hidden_size, config.hidden_size)

    def forward(self, x: torch.Tensor, attention_mask: Optional[torch.Tensor] = None):
        """多尺度注意力前向传播"""
        outputs = []

        for scale, attention in zip(self.scales, self.attentions):
            if scale == 1:
                # 原始尺度
                outputs.append(attention(x, attention_mask))
            else:
                # 下采样到不同尺度
                downsampled = x[:, ::scale, :]
                attn_out = attention(downsampled, None)
                # 上采样回原始尺度
                upsampled = torch.repeat_interleave(attn_out, scale, dim=1)
                # 截断或填充到原始长度
                if upsampled.size(1) > x.size(1):
                    upsampled = upsampled[:, : x.size(1), :]
                elif upsampled.size(1) < x.size(1):
                    pad_length = x.size(1) - upsampled.size(1)
                    upsampled = torch.nn.functional.pad(
                        upsampled, (0, 0, 0, pad_length)
                    )
                outputs.append(upsampled)

        # 融合多尺度特征
        combined = torch.cat(outputs, dim=-1)
        return self.fusion(combined)
