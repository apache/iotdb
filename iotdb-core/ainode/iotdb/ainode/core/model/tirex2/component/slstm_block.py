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

"""sLSTM layers and configuration helpers.

The recurrence runs on the vendored slstm-kernels: PyTorch on CPU/MPS (and on CUDA
without Triton) and Triton on CUDA.
"""

import math
from dataclasses import dataclass, field
from math import sqrt
from typing import Literal

import torch
from torch import nn

from iotdb.ainode.core.model.tirex2.component import kernel
from iotdb.ainode.core.model.tirex2.component.norm import MultiHeadLayerNorm


class HeadwiseLinear(nn.Module):
    """Per-head linear projections for all gates at once: [..., H, E] -> [..., G, H, O]."""

    def __init__(self, num_heads: int, head_dim: int, num_gates=4):
        super().__init__()
        self.weight = nn.Parameter(
            torch.empty(num_gates, num_heads, head_dim, head_dim)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.einsum("...HE,GHEO->...GHO", x, self.weight.transpose(-1, -2))


# Copied from xlstm.components.init.
def small_init_init_(param: torch.Tensor, dim: int) -> torch.Tensor:
    """Fills the input Tensor with values according to the method described in Transformers without Tears: Improving
    the Normalization of Self-Attention - Nguyen, T. & Salazar, J. (2019), using a normal distribution.
    Adopted from https://github.com/EleutherAI/gpt-neox/blob/main/megatron/model/init_functions.py.
    """
    std = math.sqrt(2 / (5 * dim))
    torch.nn.init.normal_(param, mean=0.0, std=std)
    return param


@dataclass
class sLSTMLayerConfig:
    """Configuration for the sLSTM layers used inside TiRex."""

    embedding_dim: int = -1
    num_heads: int = 4  # this must divide the embedding_dim
    conv1d_kernel_size: int = 0  # 0 means no convolution included
    dropout: float = 0.0

    # Cell specific inits
    recurrent_weight_init: str = "standard"
    bias_init: str = "powerlaw_blockdependent"

    # The vendored slstm-kernels implementation: "triton" (CUDA) or "torch" (any device); "auto" picks
    # "triton" for CUDA inputs when Triton is available and "torch" otherwise.
    backend: Literal["auto", "triton", "torch"] = "auto"

    # The sLSTM has 4 gates and 4 states (y, c, n, m)
    num_gates_i: int = field(default=4, init=False)
    num_states: int = field(default=4, init=False)

    def __post_init__(self):
        """Validate dimensions and derive head information."""
        if self.backend not in ("auto", "triton", "torch"):
            raise ValueError(
                f"sLSTM backend must be 'auto', 'triton' or 'torch', got {self.backend!r}."
            )
        assert self.embedding_dim % self.num_heads == 0
        self.hidden_dim = self.embedding_dim
        self.head_dim = self.embedding_dim // self.num_heads


class sLSTMLayer(nn.Module):
    """sLSTM layer: gate projections, the recurrence (see ``slstm_kernels``) and a group norm."""

    config_class = sLSTMLayerConfig

    def __init__(self, config: sLSTMLayerConfig, block_idx: int, num_blocks: int):
        super().__init__()
        self.config = config

        if self.config.conv1d_kernel_size > 0:
            raise NotImplementedError(
                "The sLSTM layer no longer supports a causal convolution (conv1d_kernel_size > 0)."
            )

        # Input projections of the f, i, z and o gates, in that order
        self.gate_proj = HeadwiseLinear(
            self.config.num_heads, self.config.head_dim, self.config.num_gates_i
        )

        self.group_norm = MultiHeadLayerNorm(
            num_heads=self.config.num_heads,
            head_dim=self.config.head_dim,
            eps=1e-6,
        )
        self.dropout = nn.Dropout(self.config.dropout)

        assert self.config.recurrent_weight_init in ["zeros", "standard"]
        assert self.config.bias_init in [
            "powerlaw_blockdependent",
            "zeros",
            "standard",
            "small_init",
        ]
        self._block_idx = block_idx
        self._num_blocks = num_blocks

        self._recurrent_kernel_ = nn.Parameter(
            torch.empty(
                self.config.num_heads,
                self.config.head_dim,
                self.config.num_gates_i,
                self.config.head_dim,
            )
        )
        self._bias_ = nn.Parameter(
            torch.empty(
                self.config.num_heads, self.config.num_gates_i, self.config.head_dim
            )
        )

        self.reset_parameters()

    def _load_from_state_dict(self, state_dict, prefix, *args, **kwargs):
        # Older checkpoints also store the recurrent kernel under this alias.
        state_dict.pop(f"{prefix}recurrent_kernel", None)
        # Older checkpoints store one [H, D_out, D_in] weight per gate
        legacy_keys = [
            f"{prefix}{gate}.weight" for gate in ("fgate", "igate", "zgate", "ogate")
        ]
        if all(key in state_dict for key in legacy_keys):
            state_dict[f"{prefix}gate_proj.weight"] = torch.stack(
                [state_dict.pop(key) for key in legacy_keys]
            )
        super()._load_from_state_dict(state_dict, prefix, *args, **kwargs)

    @torch.no_grad()
    def reset_weights(self):
        """Reset recurrent kernels according to the chosen scheme."""
        if self.config.recurrent_weight_init == "zeros":
            nn.init.zeros_(self._recurrent_kernel_)
        elif self.config.recurrent_weight_init == "standard":
            bound = 1.0 / sqrt(self.config.hidden_dim)
            nn.init.uniform_(self._recurrent_kernel_, -bound, bound)

    @torch.no_grad()
    def reset_bias(self):
        """Reset gate biases with power-law schedule for the forget gate."""
        if self.config.bias_init == "zeros":
            nn.init.zeros_(self._bias_)
        elif self.config.bias_init == "powerlaw_blockdependent":
            ratio_0_to_1 = (
                self._block_idx / (self._num_blocks - 1)
                if self._num_blocks > 1
                else 0.0
            )
            positions = torch.arange(self.config.head_dim) / (self.config.head_dim - 1)
            nn.init.zeros_(self._bias_)
            # gate order is i, f, z, o
            self._bias_[:, 1, :] = -(
                -5.0 + 12.0 * positions ** (0.3 + 1.3 * ratio_0_to_1)
            )

    def get_R(self):
        """Return the recurrent kernel [H, D_in, G, D_out]."""
        return self._recurrent_kernel_

    def get_bias(self):
        """Return the gate bias [H, G, D]."""
        return self._bias_

    def zero_state(self, batch_dim, input_):
        """Allocate zero states for convolutional and recurrent parts."""
        return torch.zeros(
            (
                self.config.num_states,
                batch_dim,
                1,
                self.config.num_heads,
                self.config.head_dim,
            ),
            dtype=input_.dtype,
            device=input_.device,
        )

    def recurrence(self, Wx: torch.Tensor, states: torch.Tensor | None = None):
        return self.slstm_kernels(
            Wx, self.get_R(), self.get_bias(), states, self.config.backend
        )

    @staticmethod
    def slstm_kernels(
        Wx: torch.Tensor,  # [B, T, G, H, D]
        R: torch.Tensor,  # [H, D_in, G, D_out] ("HPGD")
        b: torch.Tensor,  # [H, G, D]
        states: torch.Tensor | None = None,  # [4, B, 1, H, D]
        backend: Literal["auto", "triton", "torch"] = "auto",
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Run the recurrence on the vendored slstm-kernels, in FlashRNN's tensor layouts.

        Both match FlashRNN's sLSTM up to the layouts, keep the states in float32
        and run the recurrent matmul on bfloat16 operands.
        """
        if backend == "auto":
            backend = "triton" if kernel.use_triton(Wx) else "torch"
        # Imported lazily, as triton is not available on every platform.
        if backend == "triton":
            from iotdb.ainode.core.model.tirex2.component.kernel.slstm_triton import (
                slstm_triton_fw as slstm_fw,
            )
        else:
            from iotdb.ainode.core.model.tirex2.component.kernel.slstm_torch import (
                slstm_torch as slstm_fw,
            )

        B, T, G, H, D = Wx.shape
        x = Wx.permute(0, 1, 3, 2, 4).contiguous()  # [B, T, H, G, D]
        # Enforce layout even when R is already BF16 and .to() returns the same view.
        weight = (
            R.permute(0, 2, 3, 1)
            .to(torch.bfloat16, memory_format=torch.contiguous_format)
            .contiguous()
        )  # [H, G, D_out, D_in]
        bias = b.to(torch.float32).contiguous()
        if states is None:
            states = torch.zeros((B, H, G, D), dtype=torch.float32, device=Wx.device)
        else:
            states = (
                states[:, :, 0].permute(1, 2, 0, 3).to(torch.float32).contiguous()
            )  # [B, H, 4, D]
        y, last_states = slstm_fw(x, states, weight, bias)
        # Like FlashRNN: per-step states [S, B, T, H, D], of which callers read y[0].
        return y.unsqueeze(0), last_states.permute(2, 0, 1, 3).unsqueeze(2)

    def reset_parameters(self):
        """Reset projections, recurrent weights, and biases."""
        small_init_init_(self.gate_proj.weight, dim=self.config.embedding_dim)
        self.reset_weights()
        self.reset_bias()

    def gate_preacts(self, x: torch.Tensor) -> torch.Tensor:
        """Project [B, T, H * D] inputs to the gate pre-activations [B, T, G, H, D]."""
        return self.gate_proj(
            x.unflatten(-1, (self.config.num_heads, self.config.head_dim))
        )

    def get_state(self, init_state, batch_dim, input_):
        """Return ``init_state`` when provided, else allocate zeros."""
        if init_state is not None:
            return init_state
        return self.zero_state(batch_dim, input_)

    def step(
        self,
        x: torch.Tensor,
        conv_state: torch.Tensor | None = None,
        slstm_state: torch.Tensor | None = None,
    ):
        """Perform a single recurrent update."""
        batch_size, _, _ = x.shape

        Wx = self.gate_preacts(x)
        y, slstm_state = self.recurrence(
            Wx, states=self.get_state(slstm_state, batch_size, x)
        )
        y = y[0]

        y = self.dropout(y)

        out = self.group_norm(y)

        return out, {"conv_state": conv_state, "slstm_state": slstm_state}

    def forward(
        self,
        x: torch.Tensor,
        **kwargs,
    ) -> torch.Tensor:
        """Process a full sequence through the sLSTM."""
        Wx = self.gate_preacts(x)
        y, _ = self.recurrence(Wx)
        y = y[0]

        y = self.dropout(y)

        out = self.group_norm(y)
        return out
