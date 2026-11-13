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

import torch
from torch import nn


class RMSNorm(nn.Module):
    def __init__(self, num_features: int, eps: float = 1e-6):
        super().__init__()
        self.num_features = num_features
        self.eps = eps
        self.weight = nn.Parameter(torch.ones(num_features))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        in_dtype = x.dtype
        x = x * torch.rsqrt(x.float().pow(2).mean(dim=-1, keepdim=True) + self.eps)
        return self.weight * x.to(in_dtype)


class MultiHeadLayerNorm(nn.Module):
    def __init__(self, num_heads: int, head_dim: int, eps: float = 1e-6):
        super().__init__()
        self.num_heads = num_heads
        self.head_dim = head_dim
        self.eps = eps
        self.weight = nn.Parameter(torch.ones(num_heads * head_dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        B, S, NH, DH = x.shape
        assert (
            NH == self.num_heads
        ), f"Expected {self.num_heads} heads, got {NH}, input shape: {x.shape}"
        assert (
            DH == self.head_dim
        ), f"Expected {self.head_dim} head dimension, got {DH}, input shape: {x.shape}"

        in_dtype = x.dtype
        x = x.float()
        x_centered = x - x.mean(dim=-1, keepdim=True)
        x = x_centered * torch.rsqrt(
            x.var(dim=-1, keepdim=True, unbiased=False) + self.eps
        )
        x = x.to(in_dtype).reshape(B, S, -1)
        return self.weight * x
