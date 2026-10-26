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

from functools import reduce
from typing import NamedTuple

import numpy as np
import torch
import torch.utils.data
from einops import repeat
from jaxtyping import Bool, Float, Int, Shaped


def pad_array(
    values: Shaped[torch.Tensor, "*batch variates series_len"],  # noqa: F722
    patch_stride: int,
) -> Shaped[torch.Tensor, "*batch variates padded_length"]:  # noqa: F722
    """
    Makes sure that the series length is divisible by the patch_stride
    by adding left-padding.
    """
    if isinstance(values, np.ndarray):
        values = torch.from_numpy(values)
    series_len = values.shape[-1]
    padded_length = int(np.ceil(series_len / patch_stride) * patch_stride)
    if values.ndim == 2:
        padded_values = torch.zeros((values.shape[0], padded_length), dtype=values.dtype, device=values.device)
    elif values.ndim == 3:
        padded_values = torch.zeros(
            (values.shape[0], values.shape[1], padded_length),
            dtype=values.dtype,
            device=values.device,
        )
    else:
        raise ValueError(f"Unsupported number of dimensions: {values.ndim}")
    padded_values[..., -series_len:] = values

    return padded_values


def pad_id_mask(
    id_mask: Int[torch.Tensor, "*batch variates series_len"],  # noqa: F722
    patch_stride: int,
) -> Int[torch.Tensor, "*batch variates padded_length"]:  # noqa: F722
    """
    Makes sure that the series length is divisible by the patch_stride
    by adding left-padding to the id mask.
    """
    series_len = id_mask.shape[-1]
    padded_length = int(np.ceil(series_len / patch_stride) * patch_stride)
    padding_amount = padded_length - series_len
    left_edge: Int[torch.Tensor, "*batch variates"] = id_mask[..., 0]  # noqa: F722
    if id_mask.ndim == 2:
        padding = repeat(
            left_edge,
            "variates -> variates padding_amount",
            padding_amount=padding_amount,
        )
        id_mask = torch.cat([padding, id_mask], dim=1)
    elif id_mask.ndim == 3:
        padding = repeat(
            left_edge,
            "batch variates -> batch variates padding_amount",
            padding_amount=padding_amount,
        )
        id_mask = torch.cat([padding, id_mask], dim=2)
    else:
        raise ValueError(f"Unsupported number of dimensions: {id_mask.ndim}")

    return id_mask


class MaskedTimeseries(NamedTuple):
    series: Float[torch.Tensor, "*batch variates series_len"]  # noqa: F722
    padding_mask: Bool[torch.Tensor, "*batch variates series_len"]  # noqa: F722
    id_mask: Int[torch.Tensor, "*batch variates #series_len"]  # noqa: F722
    timestamp_seconds: Int[torch.Tensor, "*batch variates series_len"]  # noqa: F722
    time_interval_seconds: Int[torch.Tensor, "*batch variates"]  # noqa: F722
    num_exogenous_variables: int = 0

    def to(self, device: torch.device) -> "MaskedTimeseries":
        return MaskedTimeseries(
            series=self.series.to(device),
            padding_mask=self.padding_mask.to(device),
            id_mask=self.id_mask.to(device),
            timestamp_seconds=self.timestamp_seconds.to(device),
            time_interval_seconds=self.time_interval_seconds.to(device),
            num_exogenous_variables=self.num_exogenous_variables,
        )


def is_extreme_value(t: torch.Tensor) -> torch.Tensor:
    if torch.is_floating_point(t):
        max_value = torch.finfo(t.dtype).max
    else:
        max_value = torch.iinfo(t.dtype).max

    return reduce(
        torch.logical_or,
        (
            torch.isinf(t),
            torch.isnan(t),
            t.abs() >= max_value / 2,
        ),
    )


def replace_extreme_values(t: torch.Tensor, replacement: float = 0.0) -> torch.Tensor:
    return torch.where(is_extreme_value(t), torch.tensor(replacement, dtype=t.dtype, device=t.device), t)
