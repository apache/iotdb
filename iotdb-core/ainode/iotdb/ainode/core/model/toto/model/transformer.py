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

import warnings
from typing import Literal, Optional, Union, cast

import torch
import torch.nn.functional as F
from einops import rearrange
from jaxtyping import Bool, Float, Int
from rotary_embedding_torch import RotaryEmbedding

from .attention import (
    AttentionAxis,
    MultiHeadAttention,
    SpaceWiseMultiheadAttention,
    TimeWiseMultiheadAttention,
)
from .feed_forward import SwiGLU
from .fusion import Fusion
from .rope import TimeAwareRotaryEmbedding
from .util import KVCache, RMSNorm, make_batched_block_mask

try:
    from xformers.ops.swiglu_op import SwiGLU as SwiGLU_fused

    XFORMERS_SWIGLU_AVAILABLE = True
except ImportError:
    warnings.warn(
        "xFormers fused SwiGLU kernel not found. "
        "Using native PyTorch implementation for feed-forward layers.",
        ImportWarning,
    )
    XFORMERS_SWIGLU_AVAILABLE = False


class TransformerLayer(torch.nn.Module):
    embed_dim: int
    num_heads: int
    mlp_hidden_dim: int
    dropout: float
    attention_axis: AttentionAxis

    def __init__(
        self,
        embed_dim: int,
        num_heads: int,
        mlp_hidden_dim: int,
        dropout: float,
        rotary_emb: RotaryEmbedding = None,
        attention_axis: AttentionAxis = AttentionAxis.TIME,
        RMS_norm: bool = True,
        use_memory_efficient_attention: bool = True,
    ):
        super().__init__()
        self.embed_dim = embed_dim
        self.num_heads = num_heads
        self.mlp_hidden_dim = mlp_hidden_dim
        self.dropout = dropout
        self.attention_axis = attention_axis

        if RMS_norm:
            self.norm1: Union[RMSNorm, torch.nn.LayerNorm] = RMSNorm(embed_dim)
            self.norm2: Union[RMSNorm, torch.nn.LayerNorm] = RMSNorm(embed_dim)
        else:
            self.norm1 = torch.nn.LayerNorm(embed_dim)
            self.norm2 = torch.nn.LayerNorm(embed_dim)

        self.attention: MultiHeadAttention

        if attention_axis == AttentionAxis.TIME:
            self.attention = TimeWiseMultiheadAttention(
                embed_dim=embed_dim,
                num_heads=num_heads,
                dropout=dropout,
                rotary_emb=rotary_emb,
                use_memory_efficient_attention=use_memory_efficient_attention,
            )
        elif attention_axis == AttentionAxis.SPACE:
            self.attention = SpaceWiseMultiheadAttention(
                embed_dim=embed_dim,
                num_heads=num_heads,
                dropout=dropout,
                rotary_emb=None,
                use_memory_efficient_attention=use_memory_efficient_attention,
            )
        else:
            raise ValueError("Invalid attention axis")

        if XFORMERS_SWIGLU_AVAILABLE:
            self.mlp = torch.nn.Sequential(
                SwiGLU_fused(in_features=embed_dim, hidden_features=mlp_hidden_dim),
                torch.nn.Dropout(dropout),
            )
        else:
            self.mlp = torch.nn.Sequential(
                torch.nn.Linear(embed_dim, 2 * mlp_hidden_dim),
                SwiGLU(),
                torch.nn.Linear(mlp_hidden_dim, embed_dim),
                torch.nn.Dropout(dropout),
            )

    def forward(
        self,
        layer_idx: int,
        inputs: Float[torch.Tensor, "batch variate seq_len embed_dim"],
        attention_mask: Optional[
            Union[
                Bool[torch.Tensor, "batch seq_len variate variate"],
                Bool[torch.Tensor, "batch #variate seq_len seq_len"],
            ]
        ] = None,
        kv_cache: Optional[KVCache] = None,
    ) -> Float[torch.Tensor, "batch variate seq_len embed_dim"]:
        pre_norm_1 = self.norm1(inputs)
        hidden_state = (
            inputs
            + self.attention(
                layer_idx, pre_norm_1, attention_mask, kv_cache
            ).contiguous()
        )

        pre_norm_2 = self.norm2(hidden_state)
        return hidden_state + self.mlp(pre_norm_2)


class Transformer(torch.nn.Module):
    def __init__(
        self,
        num_layers: int,
        embed_dim: int,
        num_heads: int,
        mlp_hidden_dim: int,
        dropout: float,
        spacewise_every_n_layers: int,
        spacewise_first: bool,
        use_memory_efficient_attention: bool = True,
        *,
        fusion: Optional[Fusion] = None,
    ):
        super().__init__()

        assert (
            embed_dim % num_heads == 0
        ), "Embedding dimension must be divisible by number of heads."

        self.rotary_emb = TimeAwareRotaryEmbedding(
            embed_dim // num_heads,
            use_xpos=True,
            cache_if_possible=True,
            seq_before_head_dim=use_memory_efficient_attention,
        )
        attention_axes = self._get_layer_types(
            num_layers, spacewise_every_n_layers, spacewise_first
        )

        self.use_memory_efficient_attention = use_memory_efficient_attention
        self.fusion = fusion

        self.layers = torch.nn.ModuleList(
            [
                TransformerLayer(
                    embed_dim=embed_dim,
                    num_heads=num_heads,
                    mlp_hidden_dim=mlp_hidden_dim,
                    dropout=dropout,
                    rotary_emb=self.rotary_emb,
                    attention_axis=attention_axes[i],
                    use_memory_efficient_attention=self.use_memory_efficient_attention,
                )
                for i in range(num_layers)
            ]
        )

    def _get_mask(
        self,
        num_heads: int,
        dtype: torch.dtype,
        id_mask: Optional[torch.Tensor] = None,
    ) -> Union[
        Bool[torch.Tensor, "batch num_heads seq_len seq_len"],
        Float[torch.Tensor, "batch num_heads seq_len seq_len"],
        Bool[torch.Tensor, "batch num_heads variate variate"],
        Float[torch.Tensor, "batch num_heads variate variate"],
    ]:
        if id_mask is None:
            raise ValueError("id_mask must be provided for spacewise masks.")

        mask = make_batched_block_mask(id_mask.transpose(-1, -2))

        if self.use_memory_efficient_attention:
            mask = self._pad_to_multiple(mask)
        mask = (
            mask.float()
            .masked_fill(~mask, float("-inf"))
            .masked_fill(mask, 0.0)
            .to(dtype)
        )

        mask = rearrange(
            mask,
            "batch seq_len variate1 variate2 -> (batch seq_len) 1 variate1 variate2",
        )
        return mask.expand(-1, num_heads, -1, -1).contiguous()

    def _pad_to_multiple(
        self,
        tensor: torch.Tensor,
        multiple: int = 8,
        causal: bool = False,
    ) -> torch.Tensor:
        pad_amount = (multiple - tensor.shape[-1] % multiple) % multiple
        if pad_amount > 0:
            new_size = tensor.shape[-1] + pad_amount
            if causal:
                full_mask = torch.tril(
                    torch.ones(
                        (new_size, new_size), dtype=tensor.dtype, device=tensor.device
                    )
                )
                full_mask[: tensor.shape[-1], : tensor.shape[-1]] = tensor
                tensor = full_mask
            else:
                tensor = F.pad(tensor, (0, pad_amount, 0, pad_amount))
        return tensor

    def _get_layer_types(
        self,
        num_layers: int,
        spacewise_every_n_layers: int,
        spacewise_first: bool,
    ) -> list[AttentionAxis]:
        if spacewise_every_n_layers == -1:
            return [AttentionAxis.TIME] * num_layers
        assert num_layers % spacewise_every_n_layers == 0

        block = [AttentionAxis.TIME] * (spacewise_every_n_layers - 1)

        if spacewise_first:
            block = [AttentionAxis.SPACE] + block
        else:
            block = block + [AttentionAxis.SPACE]

        return block * (num_layers // spacewise_every_n_layers)

    def forward(
        self,
        inputs: Float[torch.Tensor, "batch variate seq_len embed_dim"],
        id_mask: Float[torch.Tensor, "batch #variate seq_len"],
        kv_cache: Optional[KVCache] = None,
        variate_label_embeds: Optional[
            Float[torch.Tensor, "batch variate 1 embed_dim"]
        ] = None,
    ) -> Float[torch.Tensor, "batch variate seq_len embed_dim"]:

        if self.fusion is not None and variate_label_embeds is not None:
            should_apply_fusion = True
            if kv_cache is not None:
                kv_len_tensor = kv_cache.current_len(0)
                kv_len = (
                    int(kv_len_tensor)
                    if isinstance(kv_len_tensor, torch.Tensor)
                    else kv_len_tensor
                )
                should_apply_fusion = kv_len == 0
            if should_apply_fusion:
                inputs = self.fusion(inputs, variate_label_embeds=variate_label_embeds)

        batch, _, seq_len, _ = inputs.shape

        if id_mask is not None and id_mask.shape[-1] != seq_len:
            added = int(seq_len - id_mask.shape[-1])
            if added > 0:
                pad_slice = id_mask[..., :1]
                id_mask = torch.cat([pad_slice.expand(-1, -1, added), id_mask], dim=-1)

        seq_len = (kv_cache.seq_len(1) if kv_cache else 0) + seq_len

        num_heads: int = cast(int, self.layers[0].num_heads)

        timewise_attention_mask = None

        spacewise_attention_mask = self._get_mask(
            num_heads=num_heads,
            dtype=inputs.dtype,
            id_mask=id_mask,
        )

        for layer_idx, layer in enumerate(self.layers):
            inputs = layer(
                layer_idx,
                inputs,
                (
                    timewise_attention_mask
                    if layer.attention_axis == AttentionAxis.TIME
                    else spacewise_attention_mask
                ),
                kv_cache,
            )
        return inputs
