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

"""Patch-based tokenization utilities for TiRex models."""

import torch
from torch import nn


class Tokenizer:
    """Tokenizer that applies sliding-window patching with optional left padding.

    Parameters
    ----------
    output_patch_size : int
        Number of timesteps produced per output patch.
    input_patch_size : int
        Window size used when unfolding the input sequence.
    input_patch_stride : int
        Stride between successive input patches.
    """

    def __init__(
        self,
        output_patch_size: int,
        input_patch_size: int,
        input_patch_stride: int,
    ):
        super().__init__()
        self.output_patch_size = output_patch_size
        self.input_patch_size = input_patch_size
        self.input_patch_stride = input_patch_stride
        self.patcher = Patch(
            self.input_patch_size, self.input_patch_stride, left_pad=True
        )

    def input_transform(self, data: torch.Tensor):
        """Patchify the 2D tensor and record patch count in state."""
        assert data.ndim == 2
        data = self.patcher(data)
        return data, {"len": data.shape[-2]}

    def output_transform(self, data: torch.Tensor, tokenizer_state: dict):
        """Reshape patched outputs back to the expected temporal layout."""
        data = torch.reshape(
            data, (data.shape[0], -1, self.output_patch_size * tokenizer_state["len"])
        )
        return data


class Patch(nn.Module):
    """Utility module that unfolds 1D tensors into patches with optional padding.

    Parameters
    ----------
    patch_size : int
        Size of each extracted patch.
    patch_stride : int
        Step between adjacent patches; must divide ``patch_size``.
    left_pad : bool
        If ``True``, pad on the left; otherwise pad on the right.
    """

    def __init__(
        self, patch_size: int, patch_stride: int, left_pad: bool = False
    ) -> None:
        super().__init__()
        self.patch_size = patch_size
        self.patch_stride = patch_stride
        self.left_pad = left_pad
        assert self.patch_size % self.patch_stride == 0

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Pad if needed and unfold the last dimension into patches."""
        assert x.ndim == 2
        length = x.shape[-1]

        if length < self.patch_size or (length % self.patch_stride != 0):
            if length < self.patch_size:
                padding_size = (
                    *x.shape[:-1],
                    self.patch_size - (length % self.patch_size),
                )
            else:
                padding_size = (
                    *x.shape[:-1],
                    self.patch_stride - (length % self.patch_stride),
                )
            padding = torch.full(
                size=padding_size, fill_value=torch.nan, dtype=x.dtype, device=x.device
            )
            if self.left_pad:
                x = torch.concat((padding, x), dim=-1)
            else:
                x = torch.concat((x, padding), dim=-1)

        x = x.unfold(dimension=-1, size=self.patch_size, step=self.patch_stride)
        return x
