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

import math

import torch
from jaxtyping import Float, Int
from torch import nn


class SinusoidalPositionEncoding(nn.Module):
    def __init__(
        self,
        *,
        width: int,
        max_len: int,
        normalize: bool = True,
    ):
        """
        Construct a sinusoidal positional embedding module.

        :param width:
            Width of the embedding.
        :param max_len:
            Maximum length of the embedding.
        :param normalize:
            Perform L2 normalization of the embedding.
        """
        super().__init__()

        position = torch.arange(max_len).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, width, 2) * (-math.log(10000.0) / width))

        pe = torch.zeros(max_len, width)
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)

        if normalize:
            l2 = torch.linalg.vector_norm(pe, dim=-1)
            pe /= l2.unsqueeze(-1)

        self.register_buffer("pe", pe, persistent=False)

    def forward(
        self, pos_id: Int[torch.Tensor, "*batch length"]
    ) -> Float[torch.Tensor, "*batch length dim"]:
        return self.pe[pos_id]


class LearnedEmbedding(nn.Module):
    def __init__(
        self,
        *,
        width: int,
        max_len: int,
    ):
        super().__init__()
        self.pe = nn.Embedding(
            max_len,
            width,
        )

    def forward(
        self, pos_id: Int[torch.Tensor, "*batch length"]
    ) -> Float[torch.Tensor, "*batch length dim"]:
        return self.pe(pos_id)
