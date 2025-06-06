import math
from typing import Optional

import torch
import torch.nn as nn


class RoPEEmbedding(nn.Module):
    """TimerXL专用的旋转位置编码"""

    def __init__(self, dim: int, max_seq_len: int = 10000, theta: float = 10000.0):
        super().__init__()
        self.dim = dim
        self.theta = theta

        if dim % 2 != 0:
            raise ValueError(f"RoPE dimension must be even, got {dim}")

        inv_freq = 1.0 / (theta ** (torch.arange(0, dim, 2).float() / dim))
        self.register_buffer("inv_freq", inv_freq)

    def forward(self, x: torch.Tensor, seq_len: int):
        device = x.device
        dtype = x.dtype

        seq_idx = torch.arange(seq_len, device=device, dtype=dtype)
        freqs = torch.outer(seq_idx, self.inv_freq)

        cos = freqs.cos()
        sin = freqs.sin()

        cos = torch.cat([cos, cos], dim=-1)
        sin = torch.cat([sin, sin], dim=-1)

        return self._apply_rotary_pos_emb(x, cos, sin)

    def _apply_rotary_pos_emb(
        self, x: torch.Tensor, cos: torch.Tensor, sin: torch.Tensor
    ):
        if x.dim() == 4:
            if x.shape[1] == cos.shape[0]:
                seq_dim = 1
                head_dim = x.shape[-1]
            else:
                seq_dim = 2
                head_dim = x.shape[-1]
        else:
            raise ValueError(f"Expected 4D tensor, got {x.dim()}D")

        target_shape = list(x.shape)
        cos_sin_shape = [1] * len(target_shape)
        cos_sin_shape[seq_dim] = cos.shape[0]
        cos_sin_shape[-1] = head_dim

        cos = cos[: target_shape[seq_dim], :head_dim].view(cos_sin_shape)
        sin = sin[: target_shape[seq_dim], :head_dim].view(cos_sin_shape)

        x1 = x[..., : head_dim // 2]
        x2 = x[..., head_dim // 2 :]

        cos1 = cos[..., : head_dim // 2]
        cos2 = cos[..., head_dim // 2 :]
        sin1 = sin[..., : head_dim // 2]
        sin2 = sin[..., head_dim // 2 :]

        rotated_x1 = x1 * cos1 - x2 * sin1
        rotated_x2 = x1 * sin2 + x2 * cos2

        return torch.cat([rotated_x1, rotated_x2], dim=-1)


class TimerAttention(nn.Module):
    """TimerXL专用的注意力机制"""

    def __init__(self, config):
        super().__init__()
        self.hidden_size = config.hidden_size
        self.num_heads = config.num_attention_heads
        self.head_dim = self.hidden_size // self.num_heads
        self.dropout_rate = getattr(config, "attention_dropout", 0.0)

        assert self.hidden_size % self.num_heads == 0

        self.q_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.k_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.v_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
        self.o_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)

        self.rope = RoPEEmbedding(self.head_dim, theta=config.rope_theta)
        self.dropout = nn.Dropout(self.dropout_rate)

    def forward(self, x: torch.Tensor, attention_mask: Optional[torch.Tensor] = None):
        batch_size, seq_len, _ = x.shape

        q = self.q_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        k = self.k_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        v = self.v_proj(x).view(batch_size, seq_len, self.num_heads, self.head_dim)

        q = self.rope(q, seq_len)
        k = self.rope(k, seq_len)

        q = q.transpose(1, 2)
        k = k.transpose(1, 2)
        v = v.transpose(1, 2)

        scores = torch.matmul(q, k.transpose(-2, -1)) / math.sqrt(self.head_dim)

        if attention_mask is not None:
            scores += attention_mask

        attn_weights = torch.softmax(scores, dim=-1)
        attn_weights = self.dropout(attn_weights)

        attn_output = torch.matmul(attn_weights, v)
        attn_output = attn_output.transpose(1, 2).contiguous()
        attn_output = attn_output.view(batch_size, seq_len, self.hidden_size)

        return self.o_proj(attn_output)
