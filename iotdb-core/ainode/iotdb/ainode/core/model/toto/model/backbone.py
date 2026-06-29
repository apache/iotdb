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

from math import ceil
from typing import NamedTuple, Optional, Type, cast

import torch
from einops import rearrange, repeat
from jaxtyping import Bool, Float, Int

from .distribution import DISTRIBUTION_CLASSES_LOOKUP, DistributionOutput
from .embedding import PatchEmbedding
from .fusion import Fusion
from .scaler import scaler_types
from .transformer import Transformer
from .util import KVCache


class TotoOutput(NamedTuple):
    """
    Output of the Toto model. Contains the output distribution, the location parameters,
    and the scale parameters.
    """

    distribution: torch.distributions.Distribution
    loc: Float[torch.Tensor, "batch variate"]
    scale: Float[torch.Tensor, "batch variate"]


class TotoBackbone(torch.nn.Module):
    """
    Toto (Timeseries-Optimized Transformer for Observability) is a transformer-based model
    for multivariate time series forecasting.
    """

    def __init__(
        self,
        patch_size: int,
        stride: int,
        embed_dim: int,
        num_layers: int,
        num_heads: int,
        mlp_hidden_dim: int,
        dropout: float,
        spacewise_every_n_layers: int,
        scaler_cls: str,
        output_distribution_classes: list[str],
        spacewise_first: bool = True,
        output_distribution_kwargs: dict | None = None,
        use_memory_efficient_attention: bool = True,
        stabilize_with_global: bool = True,
        scale_factor_exponent: float = 10.0,
    ):
        super().__init__()
        self.embed_dim = embed_dim
        self.fusion: Optional[Fusion] = None
        self.num_prepended_tokens: int = 0
        self.target_variate_label: Optional[torch.nn.Parameter] = None
        self.exogenous_variate_label: Optional[torch.nn.Parameter] = None

        if scaler_cls in (
            "<class 'model.scaler.CausalPatchStdMeanScaler'>",
            "per_variate_causal_patch",
        ):
            self.scaler = scaler_types[scaler_cls](
                patch_size=patch_size,
                stabilize_with_global=stabilize_with_global,
                scale_factor_exponent=scale_factor_exponent,
            )
        else:
            self.scaler = scaler_types[scaler_cls]()

        self.patch_embed = PatchEmbedding(patch_size, stride, embed_dim)
        self.dropout = dropout
        self.num_layers = num_layers
        self.use_memory_efficient_attention = use_memory_efficient_attention
        self.transformer = Transformer(
            embed_dim=embed_dim,
            num_heads=num_heads,
            num_layers=self.num_layers,
            mlp_hidden_dim=mlp_hidden_dim,
            dropout=dropout,
            spacewise_every_n_layers=spacewise_every_n_layers,
            spacewise_first=spacewise_first,
            use_memory_efficient_attention=self.use_memory_efficient_attention,
            fusion=self.fusion,
        )
        self.unembed = torch.nn.Linear(embed_dim, embed_dim * patch_size)

        output_distribution_classes_ = [
            DISTRIBUTION_CLASSES_LOOKUP[c] for c in output_distribution_classes
        ]
        self.output_distribution = output_distribution_classes_[0](
            embed_dim, **(output_distribution_kwargs or {})
        )

    def allocate_kv_cache(
        self,
        batch_size: int,
        num_variates: int,
        max_time_steps: int,
        device: torch.device,
        dtype: torch.dtype,
    ) -> KVCache:
        return KVCache(
            batch_size=batch_size,
            num_variates=num_variates,
            transformer_layers=list(self.transformer.layers),
            num_layers=self.num_layers,
            embed_dim=self.embed_dim,
            num_heads=cast(int, self.transformer.layers[0].num_heads),
            max_seq_len=ceil(max_time_steps / self.patch_embed.stride),
            device=device,
            dtype=dtype,
            use_memory_efficient_attention=self.use_memory_efficient_attention,
        )

    def backbone(
        self,
        inputs: Float[torch.Tensor, "batch variate time_steps"],
        input_padding_mask: Bool[torch.Tensor, "batch variate time_steps"],
        id_mask: Float[torch.Tensor, "batch #variate time_steps"],
        kv_cache: Optional[KVCache] = None,
        scaling_prefix_length: Optional[int] = None,
        num_exogenous_variables: int = 0,
    ) -> tuple[
        Float[torch.Tensor, "batch variates time_steps embed_dim"],
        Float[torch.Tensor, "batch variates time_steps"],
        Float[torch.Tensor, "batch variates time_steps"],
    ]:
        scaled_inputs, loc, scale = self.scaler(
            inputs,
            weights=torch.ones_like(inputs, device=inputs.device),
            padding_mask=input_padding_mask,
            prefix_length=scaling_prefix_length,
        )

        if kv_cache is not None:
            kv_cache_len_tensor = kv_cache.current_len(0)
            kv_cache_len = (
                int(kv_cache_len_tensor)
                if isinstance(kv_cache_len_tensor, torch.Tensor)
                else kv_cache_len_tensor
            )
            prefix_len = max(
                0, self.patch_embed.stride * (kv_cache_len - self.num_prepended_tokens)
            )

            scaled_inputs = scaled_inputs[:, :, prefix_len:]

            assert (prefix_len == 0) or (
                scaled_inputs.shape[-1] == self.patch_embed.stride
            ), "Must decode one step at a time."

            input_padding_mask = input_padding_mask[:, :, prefix_len:]
            id_mask = id_mask[:, :, prefix_len:]

        embeddings, reduced_id_mask = self.patch_embed(scaled_inputs, id_mask)

        variate_label_embeds = self.build_variate_label_embeds(
            num_exogenous_variables, embeddings
        )

        original_seq_len = embeddings.shape[2]
        transformed = self.transformer(
            embeddings,
            reduced_id_mask,
            kv_cache,
            variate_label_embeds=variate_label_embeds,
        )
        added_tokens = transformed.shape[2] - original_seq_len
        if added_tokens > 0:
            transformed = transformed[:, :, added_tokens:]

        flattened: Float[torch.Tensor, "batch variates new_seq_len embed_dim"] = (
            rearrange(
                self.unembed(transformed),
                "batch variates seq_len (patch_size embed_dim) -> batch variates (seq_len patch_size) embed_dim",
                embed_dim=self.embed_dim,
            )
        )
        return flattened, loc, scale

    def forward(
        self,
        inputs: Float[torch.Tensor, "batch variate time_steps"],
        input_padding_mask: Bool[torch.Tensor, "batch variate time_steps"],
        id_mask: Float[torch.Tensor, "batch #variate time_steps"],
        kv_cache: Optional[KVCache] = None,
        scaling_prefix_length: Optional[int] = None,
        num_exogenous_variables: int = 0,
    ) -> TotoOutput:
        flattened, loc, scale = self.backbone(
            inputs,
            input_padding_mask,
            id_mask,
            kv_cache,
            scaling_prefix_length,
            num_exogenous_variables,
        )

        return TotoOutput(self.output_distribution(flattened), loc, scale)

    @property
    def device(self):
        return next(self.parameters()).device

    def enable_variate_labels(self) -> None:
        self.fusion = Fusion()
        self.num_prepended_tokens = 1
        self.target_variate_label = torch.nn.Parameter(torch.randn(self.embed_dim))
        self.exogenous_variate_label = torch.nn.Parameter(torch.randn(self.embed_dim))
        if hasattr(self, "transformer") and self.transformer is not None:
            self.transformer.fusion = self.fusion

    def build_variate_label_embeds(
        self,
        num_exogenous_variables: int,
        embeddings: Float[torch.Tensor, "batch variate seq_len embed_dim"],
    ) -> Optional[Float[torch.Tensor, "batch variate 1 embed_dim"]]:
        if self.fusion is None:
            return None

        assert self.target_variate_label is not None
        assert self.exogenous_variate_label is not None

        batch_size, num_variates, _, _ = embeddings.shape

        target_variate_label = repeat(
            self.target_variate_label, "d -> b v 1 d", b=batch_size, v=num_variates
        ).to(device=embeddings.device, dtype=embeddings.dtype)
        exogenous_variate_label = repeat(
            self.exogenous_variate_label, "d -> b v 1 d", b=batch_size, v=num_variates
        ).to(device=embeddings.device, dtype=embeddings.dtype)
        exog_mask = torch.zeros(
            1, num_variates, 1, 1, dtype=torch.bool, device=embeddings.device
        )
        if num_exogenous_variables > 0:
            exog_mask[:, -num_exogenous_variables:] = True
        return torch.where(exog_mask, exogenous_variate_label, target_variate_label)
