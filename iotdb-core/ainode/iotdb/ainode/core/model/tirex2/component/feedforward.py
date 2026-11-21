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

"""Feed-forward networks: a plain MLP and the gated FeedForward of xLSTM Large."""

import math

import torch
from torch import nn


class MLP(nn.Module):
    """Feed-forward network ``wo(dropout(act(wi(x))))`` of the attention block."""

    def __init__(
        self, dim: int, hidden_dim: int, act_fn: nn.Module, dropout: float = 0.0
    ):
        super().__init__()
        self.wi = nn.Linear(dim, hidden_dim, bias=False)
        self.wo = nn.Linear(hidden_dim, dim, bias=False)
        self.act_fn = act_fn
        self.dropout = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.wo(self.dropout(self.act_fn(self.wi(x))))


class FeedForward(nn.Module):
    """SwiGLU feed-forward network of xLSTM Large, 8/3 of the embedding rounded up to a multiple of 64."""

    def __init__(self, dim: int):
        super().__init__()
        hidden_dim = math.ceil(dim * 2.6667 / 64) * 64
        self.proj_up_gate = nn.Linear(dim, hidden_dim, bias=False)
        self.proj_up = nn.Linear(dim, hidden_dim, bias=False)
        self.proj_down = nn.Linear(hidden_dim, dim, bias=False)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.proj_down(
            nn.functional.silu(self.proj_up_gate(x)) * self.proj_up(x)
        )
