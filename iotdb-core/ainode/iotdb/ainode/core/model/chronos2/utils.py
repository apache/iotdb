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

from typing import List

import torch
from einops import repeat


def left_pad_and_stack_1D(tensors: List[torch.Tensor]) -> torch.Tensor:
    max_len = max(len(c) for c in tensors)
    padded = []
    for c in tensors:
        assert isinstance(c, torch.Tensor)
        assert c.ndim == 1
        padding = torch.full(
            size=(max_len - len(c),), fill_value=torch.nan, device=c.device
        )
        padded.append(torch.concat((padding, c), dim=-1))
    return torch.stack(padded)


def interpolate_quantiles(
    query_quantile_levels: torch.Tensor | list[float],
    original_quantile_levels: torch.Tensor | list[float],
    original_values: torch.Tensor,
) -> torch.Tensor:
    """
    Interpolates quantile values at specified query levels using linear interpolation using original
    quantile levels and their corresponding values. This behaves similar to `torch.quantile` in terms of
    the linear interpolation but also supports non-equidistant original quantile levels.

    Parameters
    ----------
    query_quantile_levels : torch.Tensor | list[float]
        The quantile levels at which to interpolate values, all levels must be between 0 and 1
    original_quantile_levels : torch.Tensor | list[float]
        The quantile levels corresponding to the original values, all levels must be between 0 and 1.
        Can be a 1D tensor or list matching the last dimension of `original_values`, or a tensor with the
        same shape as `original_values`
    original_values : torch.Tensor
        The values corresponding to the original quantile levels, can have any number of leading dimensions

    Returns
    -------
    torch.Tensor
        The interpolated quantiles at the query quantile levels. All leading dimensions have the same size
        as `original_values` and the last dimension has size `len(query_quantile_levels)`.
    """
    assert torch.is_floating_point(
        original_values
    ), "`original_values` must be a floating point tensor"
    orig_dtype = original_values.dtype
    if isinstance(query_quantile_levels, list):
        query_quantile_levels = torch.tensor(query_quantile_levels, dtype=torch.float32)
    if isinstance(original_quantile_levels, list):
        original_quantile_levels = torch.tensor(
            original_quantile_levels, dtype=torch.float32
        )

    assert (
        query_quantile_levels.ndim == 1
    ), "`query_quantile_levels` must be 1-dimensional"
    if original_quantile_levels.ndim > 1:
        assert (
            original_quantile_levels.shape == original_values.shape
        ), "If `original_quantile_levels` is not 1D, its shape must match `original_values`"
    else:
        assert (
            len(original_quantile_levels) == original_values.shape[-1]
        ), "If `original_quantile_levels` is 1D, its length must match the last dim of `original_values`"
    assert (
        query_quantile_levels.min() >= 0.0 and query_quantile_levels.max() <= 1.0
    ), "`query_quantile_levels` must be between 0 and 1"
    assert (
        original_quantile_levels.min() >= 0.0 and original_quantile_levels.max() <= 1.0
    ), "`original_quantile_levels` must be between 0 and 1"
    original_quantile_levels = torch.clamp(original_quantile_levels, min=0.0, max=1.0)

    device = original_values.device
    query_quantile_levels = query_quantile_levels.to(device)
    original_quantile_levels = original_quantile_levels.to(device)
    original_values = original_values.to(torch.float32)

    orig_values_shape = original_values.shape
    num_original_quantiles = original_quantile_levels.shape[-1]
    original_values = original_values.reshape(-1, num_original_quantiles)
    batch_size = original_values.shape[0]

    # If original_quantile_levels is 1D, expand it to match the batch dimension
    if original_quantile_levels.ndim == 1:
        original_quantile_levels = original_quantile_levels.expand(batch_size, -1)
    else:
        original_quantile_levels = original_quantile_levels.reshape(
            -1, num_original_quantiles
        )

    # Sort original quantile levels and the corresponding values
    sorted_levels, sorted_indices = torch.sort(original_quantile_levels, dim=-1)
    sorted_values = torch.gather(original_values, dim=-1, index=sorted_indices)

    # Add extreme quantiles (0., 1.) to handle extrapolation and queries at 0 or 1
    zeros_padding = torch.zeros((batch_size, 1), dtype=torch.float32, device=device)
    ones_padding = torch.ones((batch_size, 1), dtype=torch.float32, device=device)

    # Only pad when extreme quantiles are not available in original_quantile_levels
    sorted_levels_with_padding = []
    sorted_values_with_padding = []
    if original_quantile_levels.min() > 0.0:
        sorted_levels_with_padding.append(zeros_padding)
        sorted_values_with_padding.append(sorted_values[:, :1])
    sorted_levels_with_padding.append(sorted_levels)
    sorted_values_with_padding.append(sorted_values)
    if original_quantile_levels.max() < 1.0:
        sorted_levels_with_padding.append(ones_padding)
        sorted_values_with_padding.append(sorted_values[:, -1:])

    sorted_levels = torch.cat(sorted_levels_with_padding, dim=-1).contiguous()
    sorted_values = torch.cat(sorted_values_with_padding, dim=-1)

    # Shape goes from (num_queries,) to (batch_size, num_queries).
    query_levels_expanded = repeat(
        query_quantile_levels, "q -> b q", b=batch_size
    ).contiguous()

    # Find (sorted) index of smallest original quantile level strictly larger than the query quantile level
    upper_indices = torch.searchsorted(sorted_levels, query_levels_expanded, right=True)
    upper_indices = torch.clamp(upper_indices, max=sorted_levels.shape[-1] - 1)
    lower_indices = upper_indices - 1

    # Gather the lower and upper levels and values for each item in the batch
    lower_levels = torch.gather(sorted_levels, dim=1, index=lower_indices)
    upper_levels = torch.gather(sorted_levels, dim=1, index=upper_indices)
    lower_values = torch.gather(sorted_values, dim=1, index=lower_indices)
    upper_values = torch.gather(sorted_values, dim=1, index=upper_indices)

    # Perform linear interpolation
    level_diff = upper_levels - lower_levels
    weight = torch.nan_to_num(
        (query_levels_expanded - lower_levels) / level_diff, nan=0.0
    )
    interpolated_values = lower_values + weight * (upper_values - lower_values)

    final_shape = (*orig_values_shape[:-1], len(query_quantile_levels))
    return interpolated_values.reshape(final_shape).to(orig_dtype)


def weighted_quantile(
    query_quantile_levels: torch.Tensor | list[float],
    sample_weights: torch.Tensor | list[float],
    samples: torch.Tensor,
):
    """
    Computes quantiles from a distribution specified by `samples` and their corresponding probability mass
    `sample_weights`. `samples` are first sorted along the last axis and an empirical cumulative distribution
    function (CDF) is constructed. Specific `query_quantile_levels` are then interpolated using this CDF.

    Parameters
    ----------
    query_quantile_levels : torch.Tensor | list[float]
        The quantile levels to interpolate from the empirical CDF, must be between 0 and 1
    sample_weights : torch.Tensor | list[float]
        The weights corresponding to each sample, must be non-negative. The sample_weights correspond to the
        last axis of `samples` and all leading batch dimensions share the same sample weights
    samples : torch.Tensor
        The sample values used to construct the empirical CDF along the last axis. The last dim must
        match the length of `sample_weights`, can have any number of leading dimensions

    Returns
    -------
    torch.Tensor
        The interpolated quantiles at the query quantile levels. All leading dimensions have the same size
        as `samples` and the last dimension has size `len(query_quantile_levels)`.
    """
    # FIXME: this interpolation works reasonably well in practice but may not be the best way to extrapolate
    assert torch.is_floating_point(
        samples
    ), "`original_values` must be a floating point tensor"
    orig_dtype = samples.dtype
    if isinstance(query_quantile_levels, list):
        query_quantile_levels = torch.tensor(query_quantile_levels, dtype=torch.float32)
    if isinstance(sample_weights, list):
        sample_weights = torch.tensor(sample_weights, dtype=torch.float32)

    assert (
        query_quantile_levels.ndim == 1 and sample_weights.ndim == 1
    ), "`query_quantile_levels` and `sample_weights` must be 1-dimensional"
    assert (
        len(sample_weights) == samples.shape[-1]
    ), "the last dim of `samples` must be equal to the length of `sample_weights`"
    assert (
        query_quantile_levels.min() >= 0.0 and query_quantile_levels.max() <= 1.0
    ), "`query_quantile_levels` must be between 0 and 1"
    assert sample_weights.min() > 0.0, "`sample_weights` must be > 0"

    device = samples.device
    query_quantile_levels = query_quantile_levels.to(device)
    sample_weights = sample_weights.to(device)
    samples = samples.to(torch.float32)

    orig_samples_shape = samples.shape
    num_samples = len(sample_weights)
    samples = samples.reshape(-1, num_samples)
    batch_size = samples.shape[0]

    # Normalize and expand weights
    sample_weights = sample_weights / sample_weights.sum(dim=-1, keepdim=True)
    sample_weights = sample_weights.expand(batch_size, -1).contiguous()

    # Sort samples and the corresponding weights
    sorted_samples, sort_indices = torch.sort(samples, dim=-1)
    sorted_weights = torch.gather(sample_weights, dim=-1, index=sort_indices)

    # Compute cumulative weights
    cumul_weights = torch.cumsum(sorted_weights, dim=-1)
    cumul_weights = torch.clamp(cumul_weights, min=0.0, max=1.0)

    # Get interpolated quantiles
    interpolated_quantiles = interpolate_quantiles(
        query_quantile_levels=query_quantile_levels,
        original_quantile_levels=cumul_weights,
        original_values=sorted_samples,
    )

    # Reshape to original shape
    final_shape = (*orig_samples_shape[:-1], len(query_quantile_levels))
    return interpolated_quantiles.reshape(final_shape).to(dtype=orig_dtype)
