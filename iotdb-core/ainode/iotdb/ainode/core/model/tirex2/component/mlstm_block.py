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
# Copyright (c) NXAI GmbH.
# SPDX-License-Identifier: Apache-2.0

"""mLSTM layer copied from xLSTM Large, with TiRex-specific tweaks."""

from dataclasses import dataclass
from typing import Literal

import torch
import torch.nn as nn

from iotdb.ainode.core.model.tirex2.component import kernel
from iotdb.ainode.core.model.tirex2.component.norm import MultiHeadLayerNorm

# soft_cap, bias_linspace_init_ and the mLSTMLayerConfig fields are copied from xlstm.


def soft_cap(
    values: torch.Tensor, cap_value: float | torch.Tensor | None
) -> torch.Tensor:
    """Soft caps a tensor to cap_value with a scaled tanh; no cap when cap_value is None."""
    if cap_value is None:
        return values
    return cap_value * torch.tanh(values / cap_value)


def bias_linspace_init_(
    param: torch.Tensor, start: float = 3.4, end: float = 6.0
) -> torch.Tensor:
    """Linearly spaced bias init across dimensions."""
    assert (
        param.dim() == 1
    ), f"param must be 1-dimensional (typically a bias), got {param.dim()}"
    n_dims = param.shape[0]
    init_vals = torch.linspace(start, end, n_dims)
    with torch.no_grad():
        param.copy_(init_vals)
    return param


@dataclass
class mLSTMLayerConfig:
    """Configuration of the xLSTM Large mLSTM layer."""

    embedding_dim: int
    num_heads: int
    use_bias: bool = False
    norm_eps: float = 1e-6
    qk_dim_factor: float = 0.5
    v_dim_factor: float = 1.0
    gate_soft_cap: float = 15.0
    # "auto" runs the Triton kernel for CUDA inputs when Triton is available and the native one otherwise.
    mlstm_kernel: Literal["auto", "native", "triton"] = "auto"


class mLSTMLayer(nn.Module):
    """mLSTM implementation copied from xLSTM 7B."""

    def __init__(self, config: mLSTMLayerConfig):
        super().__init__()
        self.config = config

        self.v_dim = int(config.embedding_dim * config.v_dim_factor)
        self.qk_dim = int(config.embedding_dim * config.qk_dim_factor)

        # Fused input projection of q, k, v and the o, i, f gate pre-activations, in that order.
        # The i and f gates always have a bias; without use_bias the rest of the bias starts at zero.
        self.in_proj_split = (
            self.qk_dim,
            self.qk_dim,
            self.v_dim,
            self.v_dim,
            self.config.num_heads,
            self.config.num_heads,
        )
        self.in_proj = nn.Linear(
            self.config.embedding_dim, sum(self.in_proj_split), bias=True
        )
        if not self.config.use_bias:
            nn.init.zeros_(self.in_proj.bias)

        self.ogate_act_fn = nn.Sigmoid()
        if config.mlstm_kernel not in ("auto", "native", "triton"):
            raise ValueError(
                f"mLSTM kernel must be 'auto', 'native' or 'triton', got {config.mlstm_kernel!r}."
            )

        self.multihead_norm = MultiHeadLayerNorm(
            num_heads=self.config.num_heads,
            head_dim=self.v_dim // self.config.num_heads,
            eps=self.config.norm_eps,
        )
        self.out_proj = nn.Linear(
            in_features=self.v_dim,
            out_features=self.config.embedding_dim,
            bias=self.config.use_bias,
        )

        # Match mLSTMBlock.reset_parameters gate initialisation; i and f are the last rows of in_proj.
        i_rows = slice(-2 * config.num_heads, -config.num_heads)
        f_rows = slice(-config.num_heads, None)
        with torch.no_grad():
            torch.nn.init.zeros_(self.in_proj.weight[f_rows])
            bias_linspace_init_(self.in_proj.bias[f_rows], start=3.0, end=6.0)
            torch.nn.init.zeros_(self.in_proj.weight[i_rows])
            torch.nn.init.normal_(self.in_proj.bias[i_rows], mean=0.0, std=0.1)

    def _load_from_state_dict(self, state_dict, prefix, *args, **kwargs):
        # Older checkpoints store a separate linear layer per projection
        legacy = ("q", "k", "v", "ogate_preact", "igate_preact", "fgate_preact")
        if all(f"{prefix}{name}.weight" in state_dict for name in legacy):
            weights = [state_dict.pop(f"{prefix}{name}.weight") for name in legacy]
            biases = [state_dict.pop(f"{prefix}{name}.bias", None) for name in legacy]
            biases = [
                w.new_zeros(w.shape[0]) if b is None else b
                for w, b in zip(weights, biases)
            ]
            state_dict[f"{prefix}in_proj.weight"] = torch.cat(weights)
            state_dict[f"{prefix}in_proj.bias"] = torch.cat(biases)
        super()._load_from_state_dict(state_dict, prefix, *args, **kwargs)

    def _kernel(self, x: torch.Tensor):
        """The chunkwise mLSTM kernel for inputs on the device of ``x``."""
        use_triton = self.config.mlstm_kernel == "triton" or (
            self.config.mlstm_kernel == "auto" and kernel.use_triton(x)
        )
        # Imported lazily, as triton is not available on every platform.
        if use_triton:
            from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_triton import (
                mlstm_chunkwise_triton,
            )

            return mlstm_chunkwise_triton
        from iotdb.ainode.core.model.tirex2.component.kernel.mlstm_native import (
            mlstm_chunkwise_native,
        )

        return mlstm_chunkwise_native

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Process a full sequence through the mlstm."""
        assert x.ndim == 3, f"Input must have shape [B, S, D], got {x.shape}"
        B, S, _ = x.shape

        q, k, v, o_preact, i_preact, f_preact = self.in_proj(x).split(
            self.in_proj_split, dim=-1
        )
        i_preact = soft_cap(i_preact, cap_value=self.config.gate_soft_cap)
        f_preact = soft_cap(f_preact, cap_value=self.config.gate_soft_cap)

        q = q.reshape(B, S, self.config.num_heads, -1).transpose(1, 2)
        k = k.reshape(B, S, self.config.num_heads, -1).transpose(1, 2)
        v = v.reshape(B, S, self.config.num_heads, -1).transpose(1, 2)
        i_preact = i_preact.transpose(1, 2)
        f_preact = f_preact.transpose(1, 2)
        h = self._kernel(q)(
            q=q,
            k=k,
            v=v,
            i=i_preact,
            f=f_preact,
        )
        expected_h_shape = (
            B,
            self.config.num_heads,
            S,
            self.v_dim // self.config.num_heads,
        )
        assert (
            h.shape == expected_h_shape
        ), f"Got {h.shape}, expected {expected_h_shape}"

        h = h.transpose(1, 2)
        h_norm = self.multihead_norm(h)
        h_norm = h_norm.reshape(B, S, -1)

        h_out = self.ogate_act_fn(o_preact) * h_norm

        y = self.out_proj(h_out)
        return y
