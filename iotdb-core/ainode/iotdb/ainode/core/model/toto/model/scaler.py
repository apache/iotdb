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

import warnings
from typing import Tuple

import torch
from einops import reduce, repeat


class Scaler(torch.nn.Module):
    """
    Minimal base class replacing gluonts.torch.scaler.Scaler.
    Provides a __call__ interface for scaling data.
    """

    pass


class StdMeanScaler(Scaler):
    """
    Scales data to have zero mean and unit variance along a given dimension.
    """

    def __init__(
        self,
        dim: int = -1,
        keepdim: bool = True,
        minimum_scale: float = 1e-3,
    ) -> None:
        super().__init__()
        self.dim = dim
        self.keepdim = keepdim
        self.minimum_scale = minimum_scale

    def __call__(
        self,
        data: torch.Tensor,
        padding_mask: torch.Tensor,
        weights: torch.Tensor,
        prefix_length: int | None = None,
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        assert data.shape == weights.shape, "data and weights must have same shape"
        with torch.no_grad():
            if prefix_length is not None:
                prefix_mask = torch.zeros_like(weights)
                prefix_mask[..., :prefix_length] = 1.0
                weights = weights * prefix_mask

            weights = weights * padding_mask

            try:
                high_precision_data = data.to(torch.float64)
            except TypeError:
                warnings.warn(
                    f"Float64 is not supported by device {data.device}. "
                    "Using float32 instead for accumulating denominator in input scaler. "
                    "This may lead to overflow issues if the data contains extreme values.",
                    RuntimeWarning,
                )
                high_precision_data = data.to(torch.float32)

            denominator = (
                weights.sum(self.dim, keepdim=self.keepdim)
                .clamp_min(1.0)
                .to(high_precision_data.dtype)
            )
            means = (high_precision_data * weights).sum(
                self.dim, keepdim=self.keepdim
            ) / denominator
            means = torch.nan_to_num(means)

            variance = (((high_precision_data - means) * weights) ** 2).sum(
                self.dim, keepdim=self.keepdim
            ) / denominator
            scale = torch.sqrt(variance + self.minimum_scale).to(data.dtype)
            loc = means.to(data.dtype)

            return (data - loc) / scale, loc, scale


def compute_causal_statistics(
    data: torch.Tensor,
    weights: torch.Tensor,
    padding_mask: torch.Tensor,
    dim: int,
    minimum_scale: float,
    use_bessel_correction: bool = True,
    stabilize_with_global: bool = False,
    scale_factor_exponent: float = 10.0,
    prefix_length: int | None = None,
) -> Tuple[torch.Tensor, torch.Tensor]:
    assert dim == -1, "compute_causal_statistics only supports dim=-1 (last dimension)"

    with torch.no_grad():
        weights = weights * padding_mask

        try:
            high_precision_data = data.to(torch.float64)
            high_precision_weights = weights.to(torch.float64)
        except TypeError:
            warnings.warn(
                f"Float64 is not supported by device {data.device}. "
                "Using float32 instead for causal scaler calculations.",
                RuntimeWarning,
            )
            high_precision_data = data.to(torch.float32)
            high_precision_weights = weights.to(torch.float32)

        prev_deterministic = torch.are_deterministic_algorithms_enabled()
        if prev_deterministic and data.device.type == "cuda":
            torch.use_deterministic_algorithms(False)

        try:
            weighted_data = high_precision_weights * high_precision_data

            cum_weights = torch.cumsum(high_precision_weights, dim=dim)
            cum_values = torch.cumsum(weighted_data, dim=dim)

            denominator = cum_weights.clamp_min(1.0)
            causal_means = cum_values / denominator

            shifted_means = torch.zeros_like(causal_means)
            shifted_means[..., 1:] = causal_means[..., :-1]

            delta = high_precision_data - shifted_means
            increment = (
                delta * (high_precision_data - causal_means) * high_precision_weights
            )
            m_2 = torch.cumsum(increment, dim=dim)

            if use_bessel_correction:
                causal_variance = m_2 / torch.clamp(denominator - 1.0, min=1.0)
            else:
                causal_variance = m_2 / denominator

            causal_scale = torch.sqrt(causal_variance + minimum_scale)

            if stabilize_with_global:
                if prefix_length is not None:
                    prefix_mask = torch.zeros_like(weights)
                    prefix_mask[..., :prefix_length] = 1.0
                    weighted_data = weighted_data * prefix_mask
                    weights = weights * prefix_mask
                    padding_mask = padding_mask * prefix_mask

                scale_factor_min = 10.0 ** (-scale_factor_exponent)
                scale_factor_max = 10.0**scale_factor_exponent

                global_denominator = (
                    (weights * padding_mask).sum(dim, keepdim=True).clamp_min(1.0)
                )
                global_means = (weighted_data).sum(
                    dim, keepdim=True
                ) / global_denominator
                global_means = torch.nan_to_num(global_means)

                global_variance = (
                    ((high_precision_data - global_means) * weights * padding_mask) ** 2
                ).sum(dim, keepdim=True) / global_denominator
                global_scale = torch.sqrt(global_variance + minimum_scale)

                expanded_global_scale = global_scale.expand_as(causal_scale)
                min_allowed_scale = expanded_global_scale * scale_factor_min
                max_allowed_scale = expanded_global_scale * scale_factor_max

                causal_scale = torch.clamp(
                    causal_scale,
                    min=torch.max(
                        torch.tensor(minimum_scale, device=causal_scale.device),
                        min_allowed_scale,
                    ),
                    max=max_allowed_scale,
                )

            causal_means = causal_means.to(data.dtype)
            causal_scale = causal_scale.to(data.dtype)

        finally:
            if prev_deterministic and data.device.type == "cuda":
                torch.use_deterministic_algorithms(True)

        return causal_means, causal_scale


class CausalStdMeanScaler(Scaler):
    def __init__(
        self,
        dim: int = -1,
        minimum_scale: float = 0.1,
        use_bessel_correction: bool = True,
        stabilize_with_global: bool = False,
        scale_factor_exponent: float = 10.0,
    ) -> None:
        super().__init__()
        assert dim == -1, "CausalStdMeanScaler only supports dim=-1 (last dimension)"
        self.dim = dim
        self.minimum_scale = minimum_scale
        self.use_bessel_correction = use_bessel_correction
        self.stabilize_with_global = stabilize_with_global
        self.scale_factor_exponent = scale_factor_exponent

    def __call__(
        self,
        data: torch.Tensor,
        padding_mask: torch.Tensor,
        weights: torch.Tensor,
        prefix_length: int | None = None,
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        assert data.shape == weights.shape, "data and weights must have same shape"
        assert (
            len(data.shape) == 3
        ), "Input data must have shape [batch, variates, time_steps]"

        causal_means, causal_scale = compute_causal_statistics(
            data,
            weights,
            padding_mask,
            self.dim,
            self.minimum_scale,
            self.use_bessel_correction,
            self.stabilize_with_global,
            self.scale_factor_exponent,
            prefix_length,
        )

        scaled_data = (data - causal_means) / causal_scale

        return scaled_data, causal_means, causal_scale


class CausalPatchStdMeanScaler(Scaler):
    def __init__(
        self,
        dim: int = -1,
        patch_size: int = 32,
        minimum_scale: float = 0.1,
        use_bessel_correction: bool = True,
        stabilize_with_global: bool = False,
        scale_factor_exponent: float = 10.0,
    ) -> None:
        super().__init__()
        assert (
            dim == -1
        ), "CausalPatchStdMeanScaler only supports dim=-1 (last dimension)"
        self.dim = dim
        self.patch_size = patch_size
        self.minimum_scale = minimum_scale
        self.use_bessel_correction = use_bessel_correction
        self.stabilize_with_global = stabilize_with_global
        self.scale_factor_exponent = scale_factor_exponent

    def __call__(
        self,
        data: torch.Tensor,
        padding_mask: torch.Tensor,
        weights: torch.Tensor,
        prefix_length: int | None = None,
    ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        assert data.shape == weights.shape, "data and weights must have same shape"
        assert (
            len(data.shape) == 3
        ), "Input data must have shape [batch, variates, time_steps]"

        with torch.no_grad():
            time_steps = data.shape[-1]
            assert (
                time_steps % self.patch_size == 0
            ), f"Time steps ({time_steps}) must be divisible by patch size ({self.patch_size})"

            causal_means, causal_scale = compute_causal_statistics(
                data,
                weights,
                padding_mask,
                -1,
                self.minimum_scale,
                self.use_bessel_correction,
                self.stabilize_with_global,
                self.scale_factor_exponent,
                prefix_length,
            )

            means_unfolded = causal_means.unfold(-1, self.patch_size, self.patch_size)
            scales_unfolded = causal_scale.unfold(-1, self.patch_size, self.patch_size)

            patch_stats_means = means_unfolded[..., -1]
            patch_stats_scales = scales_unfolded[..., -1]

            patch_means = repeat(
                patch_stats_means, "b v p -> b v (p s)", s=self.patch_size
            )
            patch_scales = repeat(
                patch_stats_scales, "b v p -> b v (p s)", s=self.patch_size
            )

            scaled_data = (data - patch_means) / patch_scales

            return scaled_data, patch_means, patch_scales


# for deserialization of SafeTensors checkpoints
scaler_types = {
    "<class 'model.scaler.StdMeanScaler'>": StdMeanScaler,
    "<class 'model.scaler.CausalStdMeanScaler'>": CausalStdMeanScaler,
    "<class 'model.scaler.CausalPatchStdMeanScaler'>": CausalPatchStdMeanScaler,
    # Short aliases used in config.json
    "per_variate": StdMeanScaler,
    "per_variate_causal": CausalStdMeanScaler,
    "per_variate_causal_patch": CausalPatchStdMeanScaler,
}
