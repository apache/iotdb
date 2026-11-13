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

"""Random past-window standardization for stochastic normalization."""

import torch


class Scaler:
    """Normalizes using mean/variance computed over the full sequence.

    Parameters
    ----------
    eps : float
        Small constant added to avoid division by zero.
    use_arcsinh : bool
        If ``True``, apply an arcsinh squashing after standardizing.
    binaryaware : bool
        If ``True``, detect binary variates and bypass scaling for them.
    """

    def __init__(
        self,
        eps: float = 1e-8,
        use_arcsinh: bool = False,
        binaryaware: bool = False,
        **kwargs
    ) -> None:
        super().__init__()
        self.eps = eps
        self.use_arcsinh = use_arcsinh
        self.binaryaware = binaryaware

    def scale(
        self,
        x: torch.Tensor,
    ) -> tuple[torch.Tensor, tuple[torch.Tensor, torch.Tensor]]:
        """Normalize ``x`` using mean/variance over the full sequence."""
        loc = torch.nan_to_num(torch.nanmean(x, dim=-1, keepdim=True), nan=0.0)
        scale = torch.nan_to_num(
            torch.nanmean((x - loc) ** 2, dim=-1, keepdim=True).sqrt(), nan=1.0
        )
        scale = torch.clamp_min(scale, min=self.eps)

        scaled_x = (x - loc) / scale

        # Detect binary variates (only NaN, 0.0, 1.0) and bypass scaling
        if self.binaryaware:
            valid = ~torch.isnan(x)
            is_binary = ((x == 0.0) | (x == 1.0) | ~valid).all(dim=-1, keepdim=True)
            loc = torch.where(is_binary, torch.zeros_like(loc), loc)
            scale = torch.where(is_binary, torch.ones_like(scale), scale)
            scaled_x = (x - loc) / scale
        else:
            is_binary = None

        if self.use_arcsinh:
            if is_binary is not None and is_binary.any():
                scaled_x = torch.where(is_binary, scaled_x, torch.arcsinh(scaled_x))
            else:
                scaled_x = torch.arcsinh(scaled_x)

        return scaled_x, (loc, scale, is_binary)

    def re_scale(
        self,
        x: torch.Tensor,
        loc_scale: tuple[torch.Tensor, torch.Tensor, torch.Tensor | None],
    ) -> torch.Tensor:
        """Undo the normalization performed by :meth:`scale`."""
        loc, scale, is_binary = loc_scale
        if self.use_arcsinh:
            # clamp before sinh to prevent overflow: sinh(x) is stable for |x| < ~88 (float32)
            sinh_x = torch.sinh(torch.clamp(x, -20.0, 20.0))
            if is_binary is not None and is_binary.any():
                # is_binary: (B, 1) -> broadcast over quantile and time dims
                x = torch.where(is_binary[:, None], x, sinh_x)
            else:
                x = sinh_x
        return x * scale[:, None] + loc[:, None]
