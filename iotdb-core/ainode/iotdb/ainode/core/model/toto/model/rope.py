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

from typing import Optional

import torch
from einops import rearrange
from jaxtyping import Int
from rotary_embedding_torch import RotaryEmbedding, apply_rotary_emb
from rotary_embedding_torch.rotary_embedding_torch import default


def exists(val):
    return val is not None


class TimeAwareRotaryEmbedding(RotaryEmbedding):
    """
    A variant of the rotary position embedding that (optionally) uses the time index
    to compute the sinusoidal and cosine embeddings. Useful for time series data.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # If the parent stored `freqs` as a Parameter, remove it and register as a buffer
        if hasattr(self, "freqs") and isinstance(self.freqs, torch.nn.Parameter):
            freqs_data = self.freqs.data
            self._parameters.pop("freqs")
            self.register_buffer("freqs", freqs_data, persistent=False)

    def rotate_queries_and_keys(
        self,
        q: torch.Tensor,
        k: torch.Tensor,
        seq_dim: Optional[int] = None,
        seq_pos: Optional[Int[torch.Tensor, "... seq_len"]] = None,
        seq_pos_offset: int = 0,
    ):
        if seq_dim is None:
            seq_dim = self.default_seq_dim

        assert self.use_xpos
        device, dtype, seq_len = q.device, q.dtype, q.shape[seq_dim]

        seq = default(seq_pos, self.get_seq_pos(seq_len, dtype=dtype, device=device))
        seq = seq + seq_pos_offset

        freqs = self.forward(seq)

        scale = self.get_scale(seq).to(dtype)

        if seq_dim == -3:
            num_heads = q.shape[-2]
            freqs = freqs.unsqueeze(1).expand(-1, num_heads, -1)
            scale = scale.unsqueeze(1).expand(-1, num_heads, -1)

        rotated_q = apply_rotary_emb(freqs, q, scale=scale, seq_dim=seq_dim)
        rotated_k = apply_rotary_emb(freqs, k, scale=scale**-1, seq_dim=seq_dim)

        rotated_q = rotated_q.type(q.dtype)
        rotated_k = rotated_k.type(k.dtype)

        return rotated_q, rotated_k

    def get_scale(
        self,
        t: torch.Tensor,
    ):
        assert self.use_xpos

        power = (t - t.max(-1).values.unsqueeze(-1) // 2) / self.scale_base

        scale = self.scale ** rearrange(power, "... n -> ... n 1")
        scale = torch.cat((scale, scale), dim=-1)

        return scale
