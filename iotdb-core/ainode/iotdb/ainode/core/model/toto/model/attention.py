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
# This file includes code derived from DataDog/toto
# (https://github.com/DataDog/toto), licensed under the Apache-2.0 License.
# Copyright 2025 Datadog, Inc.

import logging
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional, Union

import torch
from einops import rearrange
from jaxtyping import Bool, Float, Int

from .rope import TimeAwareRotaryEmbedding

if TYPE_CHECKING:
    from .util import KVCache

log = logging.getLogger(__name__)

try:
    from xformers.ops import LowerTriangularMask, memory_efficient_attention

    XFORMERS_AVAILABLE = True
    log.info("xFormers Memory-Efficient Attention available.")
except ImportError:
    warnings.warn(
        "xFormers Memory-Efficient Attention not available. "
        "Falling back to native PyTorch scaled_dot_product_attention.",
        ImportWarning,
    )

    XFORMERS_AVAILABLE = False

from torch.nn.functional import scaled_dot_product_attention


class AttentionAxis(Enum):
    TIME = 1
    SPACE = 2


class BaseMultiheadAttention(torch.nn.Module):
    def __init__(
        self,
        embed_dim: int,
        num_heads: int,
        dropout: float,
        rotary_emb: Optional[TimeAwareRotaryEmbedding],
        use_memory_efficient_attention: bool,
    ):
        super().__init__()
        self.embed_dim = embed_dim
        self.num_heads = num_heads
        assert (
            embed_dim % num_heads == 0
        ), "Embedding dimension must be divisible by number of heads."
        self.head_dim = embed_dim // num_heads
        self.rotary_emb = rotary_emb

        self.wQKV = torch.nn.Linear(embed_dim, embed_dim * 3)
        self.dropout = dropout
        self.use_memory_efficient_attention = use_memory_efficient_attention
        self.wO = torch.nn.Linear(embed_dim, embed_dim)

        assert not (
            not XFORMERS_AVAILABLE and self.use_memory_efficient_attention
        ), "XFORMERS_AVAILABLE is False, so use_memory_efficient_attention must be False"

        if not hasattr(self, "attention_axis") or self.attention_axis not in (
            AttentionAxis.TIME,
            AttentionAxis.SPACE,
        ):
            raise ValueError(
                "Child class must define attention_axis as AttentionAxis.TIME or AttentionAxis.SPACE."
            )

    def rearrange_inputs(
        self, inputs: Float[torch.Tensor, "batch variate seq_len embed_dim"]
    ) -> Float[torch.Tensor, "... embed_dim"]:
        pattern = (
            "batch variate seq_len embed_dim -> (batch variate) seq_len embed_dim"
            if self.attention_axis == AttentionAxis.TIME
            else "batch variate seq_len embed_dim -> (batch seq_len) variate embed_dim"
        )
        return rearrange(inputs, pattern)

    def get_qkv(self, inputs: torch.Tensor) -> tuple[torch.Tensor, ...]:
        if (
            self.attention_axis == AttentionAxis.TIME
            and self.use_memory_efficient_attention
        ):
            pattern = "batch_X_variate seq_len (qkv head_dim n_heads) -> qkv batch_X_variate seq_len n_heads head_dim"
        elif (
            self.attention_axis == AttentionAxis.TIME
            and not self.use_memory_efficient_attention
        ):
            pattern = "batch_X_variate seq_len (qkv head_dim n_heads) -> qkv batch_X_variate n_heads seq_len head_dim"
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and self.use_memory_efficient_attention
        ):
            pattern = "batch_X_seq_len variate (qkv head_dim n_heads) -> qkv batch_X_seq_len variate n_heads head_dim"
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and not self.use_memory_efficient_attention
        ):
            pattern = "batch_X_seq_len variate (qkv head_dim n_heads) -> qkv batch_X_seq_len n_heads variate head_dim"

        qkv = self.wQKV(inputs.contiguous())
        return rearrange(
            qkv, pattern, qkv=3, head_dim=self.head_dim, n_heads=self.num_heads
        ).unbind(dim=0)

    def positional_embedding(self, q, k, v, kv_cache, layer_idx):
        seq_pos_offset = 0
        if self.rotary_emb is not None and self.attention_axis == AttentionAxis.TIME:
            if kv_cache is not None:
                seq_pos_offset = kv_cache.seq_len(layer_idx)
            q, k = self.rotary_emb.rotate_queries_and_keys(
                q, k, seq_pos_offset=seq_pos_offset
            )

        if kv_cache is not None and self.attention_axis == AttentionAxis.TIME:
            kv_cache.append(layer_idx, (k, v))
            k, v = kv_cache[layer_idx]

        q = q.contiguous()
        k = k.contiguous().to(q.dtype)
        v = v.contiguous().to(q.dtype)

        return q, k, v, seq_pos_offset

    def rearrange_output(
        self, output: torch.Tensor, batch: int, variate: int, seq_len: int
    ) -> Float[torch.Tensor, "batch variate seq_len embed_dim"]:
        if (
            self.attention_axis == AttentionAxis.TIME
            and self.use_memory_efficient_attention
        ):
            pattern = "(batch variate) seq_len n_heads head_dim -> batch variate seq_len (n_heads head_dim)"
        elif (
            self.attention_axis == AttentionAxis.TIME
            and not self.use_memory_efficient_attention
        ):
            pattern = "(batch variate) n_heads seq_len head_dim -> batch variate seq_len (n_heads head_dim)"
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and self.use_memory_efficient_attention
        ):
            pattern = "(batch seq_len) variate n_heads head_dim -> batch variate seq_len (n_heads head_dim)"
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and not self.use_memory_efficient_attention
        ):
            pattern = "(batch seq_len) n_heads variate head_dim -> batch variate seq_len (n_heads head_dim)"

        return rearrange(output, pattern, batch=batch, variate=variate, seq_len=seq_len)

    def run_attention(
        self, attention_mask, q, k, v, seq_pos_offset, dropout, seq_len, variate
    ):
        q_dim_start, q_dim_end = seq_pos_offset, seq_pos_offset + seq_len
        kv_dim_start, kv_dim_end = 0, (
            v.shape[1] if self.use_memory_efficient_attention else v.shape[2]
        )
        if (
            self.attention_axis == AttentionAxis.TIME
            and self.use_memory_efficient_attention
        ):
            attention_mask = (
                attention_mask[..., q_dim_start:q_dim_end, kv_dim_start:kv_dim_end]
                if torch.is_tensor(attention_mask)
                else LowerTriangularMask() if seq_pos_offset == 0 else None
            )
            return memory_efficient_attention(
                q, k, v, attn_bias=attention_mask, p=dropout
            )
        elif (
            self.attention_axis == AttentionAxis.TIME
            and not self.use_memory_efficient_attention
        ):
            attention_mask = (
                attention_mask[..., q_dim_start:q_dim_end, kv_dim_start:kv_dim_end]
                if torch.is_tensor(attention_mask)
                else None
            )
            return scaled_dot_product_attention(
                q,
                k,
                v,
                attn_mask=attention_mask,
                dropout_p=dropout,
                is_causal=(attention_mask is None and seq_pos_offset == 0),
            )
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and self.use_memory_efficient_attention
        ):
            attention_mask = (
                attention_mask[..., kv_dim_start:kv_dim_end, kv_dim_start:kv_dim_end]
                if torch.is_tensor(attention_mask)
                else None
            )
            return memory_efficient_attention(
                q, k, v, attn_bias=attention_mask, p=dropout
            )
        elif (
            self.attention_axis == AttentionAxis.SPACE
            and not self.use_memory_efficient_attention
        ):
            attention_mask = (
                attention_mask[..., kv_dim_start:kv_dim_end, kv_dim_start:kv_dim_end]
                if torch.is_tensor(attention_mask)
                else None
            )
            return scaled_dot_product_attention(
                q, k, v, attn_mask=attention_mask, dropout_p=dropout, is_causal=False
            )

    def forward(
        self,
        layer_idx: int,
        inputs: Float[torch.Tensor, "batch variate seq_len embed_dim"],
        attention_mask: Optional[
            Union[
                Bool[torch.Tensor, "batch_X_variate n_heads seq_len seq_len"],
                Bool[torch.Tensor, "batch_X_seq_len n_heads variate variate"],
            ]
        ] = None,
        kv_cache: Optional["KVCache"] = None,
    ) -> Float[torch.Tensor, "batch variate seq_len embed_dim"]:
        batch_size, variate, seq_len, _ = inputs.shape
        dropout = self.dropout if self.training else 0.0

        rearranged_inputs = self.rearrange_inputs(inputs)
        q, k, v = self.get_qkv(rearranged_inputs)

        q, k, v, seq_pos_offset = self.positional_embedding(
            q, k, v, kv_cache, layer_idx
        )

        output = self.run_attention(
            attention_mask, q, k, v, seq_pos_offset, dropout, seq_len, variate
        )

        output = self.rearrange_output(output, batch_size, variate, seq_len)
        return self.wO(output)


class TimeWiseMultiheadAttention(BaseMultiheadAttention):
    attention_axis = AttentionAxis.TIME


class SpaceWiseMultiheadAttention(BaseMultiheadAttention):
    attention_axis = AttentionAxis.SPACE


MultiHeadAttention = TimeWiseMultiheadAttention | SpaceWiseMultiheadAttention
