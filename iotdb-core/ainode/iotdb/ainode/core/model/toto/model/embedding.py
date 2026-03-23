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
from jaxtyping import Float, Int, Num


def patchify_id_mask(
    id_mask: Int[torch.Tensor, "batch variate time_steps"], patch_size: int
) -> Int[torch.Tensor, "batch variate seq_len patch_size"]:
    patched_id_mask = id_mask.unfold(dimension=-1, size=patch_size, step=patch_size)
    patched_id_mask_min = patched_id_mask.min(-1).values
    patched_id_mask_max = patched_id_mask.max(-1).values
    assert torch.eq(
        patched_id_mask_min, patched_id_mask_max
    ).all(), "Patches cannot span multiple datasets"
    return patched_id_mask_min


class PatchEmbedding(torch.nn.Module):
    """
    Multivariate time series patch embedding.
    Patchifies each variate separately.
    """

    def __init__(self, patch_size: int, stride: int, embed_dim: int):
        super().__init__()
        self.patch_size = patch_size
        self.embed_dim = embed_dim
        self.stride = stride
        self.projection = torch.nn.Linear(self.patch_size, self.embed_dim)

    def _patchify(
        self, x: Num[torch.Tensor, "batch variate time_steps"]
    ) -> Num[torch.Tensor, "batch variate seq_len patch_size"]:
        return x.unfold(dimension=-1, size=self.patch_size, step=self.stride)

    def forward(
        self,
        x: Float[torch.Tensor, "batch #variate time_steps"],
        id_mask: Float[torch.Tensor, "batch time_steps"],
    ) -> tuple[
        Float[torch.Tensor, "batch variate seq_len embed_dim"],
        Int[torch.Tensor, "batch seq_len"],
    ]:
        assert (
            x.shape[-1] % self.patch_size == 0
        ), f"Series length ({x.shape=}) must be divisible by ({self.patch_size=})"
        x_patched: Float[torch.Tensor, "batch variate seq_len patch_size"] = (
            self._patchify(x)
        )
        id_mask_patched: Int[torch.Tensor, "batch variate seq_len patch_size"] = (
            self._patchify(id_mask)
        )

        assert torch.eq(
            id_mask_patched.min(-1).values, id_mask_patched.max(-1).values
        ).all(), "Patches cannot span multiple datasets"

        return (
            self.projection(x_patched),
            id_mask_patched.min(-1).values,
        )
