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

from dataclasses import dataclass
from typing import cast

import numpy as np
import torch
from einops import rearrange, repeat
from jaxtyping import Bool, Float, Int
from torch.distributions import Distribution, TransformedDistribution
from torch.distributions.transforms import AffineTransform

from ..data.util.dataset import (
    MaskedTimeseries,
    pad_array,
    pad_id_mask,
    replace_extreme_values,
)
from ..model.backbone import TotoBackbone


class AffineTransformed(TransformedDistribution):
    """
    Thin wrapper around TransformedDistribution with AffineTransform,
    replacing gluonts.torch.distributions.AffineTransformed.
    """

    def __init__(self, base_distribution, loc=0.0, scale=1.0):
        super().__init__(base_distribution, AffineTransform(loc=loc, scale=scale))

    @property
    def mean(self):
        loc = self.transforms[0].loc
        scale = self.transforms[0].scale
        return loc + scale * self.base_dist.mean

    # Note: Do NOT override sample() here. TransformedDistribution.sample() correctly
    # calls base_dist.sample() (not rsample), which works for non-reparameterizable
    # distributions like MixtureSameFamily.


@dataclass(frozen=True)
class Forecast:
    mean: Float[torch.Tensor, "batch variate future_time_steps"]
    samples: Float[torch.Tensor, "batch variate future_time_steps samples"] | None = (
        None
    )

    def quantile(
        self, q: float | torch.Tensor
    ) -> Float[torch.Tensor, "batch variate future_time_steps"]:
        assert self.samples is not None, "samples must be provided to compute quantiles"
        assert isinstance(q, (float, torch.Tensor)), "q must be a float or a tensor"
        if isinstance(q, float):
            q = torch.tensor(q, device=self.samples.device, dtype=self.samples.dtype)
        return self.samples.quantile(q, dim=-1)

    @property
    def median(self) -> Float[torch.Tensor, "batch variate future_time_steps"]:
        return self.quantile(0.5)

    @property
    def std(self) -> Float[torch.Tensor, "batch variate future_time_steps"]:
        assert (
            self.samples is not None
        ), "samples must be provided to compute standard deviation"
        return self.samples.std(dim=-1)


class TotoForecaster:
    """
    A forecaster class for the Toto model that handles autoregressive decoding
    for time series forecasting.
    """

    model: TotoBackbone

    def __init__(self, model: TotoBackbone):
        self.model = model
        self.model.eval()

    def forecast(
        self,
        inputs: MaskedTimeseries,
        prediction_length: int,
        num_samples: int | None = None,
        samples_per_batch: int = 10,
        use_kv_cache: bool = True,
        future_exogenous_variables: (
            Float[torch.Tensor, "batch exogenous_variables future_time_steps"] | None
        ) = None,
    ) -> Forecast:
        if len(inputs.series.shape) == 2:
            batch = cast(MaskedTimeseries, torch.utils.data.default_collate([inputs]))
        else:
            batch = inputs

        if (
            future_exogenous_variables is not None
            and len(future_exogenous_variables.shape) == 2
        ):
            future_exogenous_variables = future_exogenous_variables.unsqueeze(0)

        series = pad_array(batch.series, self.model.patch_embed.stride)
        padding_mask = pad_array(batch.padding_mask, self.model.patch_embed.stride)
        id_mask = batch.id_mask
        if id_mask is not None:
            id_mask = pad_id_mask(batch.id_mask, self.model.patch_embed.stride)
        timestamp_seconds = pad_array(
            batch.timestamp_seconds, self.model.patch_embed.stride
        )
        time_interval_seconds: Int[torch.Tensor, "batch variate series_len"] = (
            torch.as_tensor(
                batch.time_interval_seconds, device=series.device, dtype=torch.int
            )
        )

        if num_samples is not None:
            samples = self.generate_samples(
                inputs=series,
                prediction_length=prediction_length,
                num_samples=num_samples,
                timestamp_seconds=timestamp_seconds,
                time_interval_seconds=time_interval_seconds,
                input_padding_mask=padding_mask,
                id_mask=id_mask,
                sampling_batch_size=samples_per_batch,
                use_kv_cache=use_kv_cache,
                future_exogenous_variables=future_exogenous_variables,
                num_exogenous_variables=batch.num_exogenous_variables,
            )
            mean = samples.mean(dim=-1)
        else:
            mean = self.generate_mean(
                inputs=series,
                prediction_length=prediction_length,
                timestamp_seconds=timestamp_seconds,
                time_interval_seconds=time_interval_seconds,
                input_padding_mask=padding_mask,
                id_mask=id_mask,
                use_kv_cache=use_kv_cache,
                future_exogenous_variables=future_exogenous_variables,
                num_exogenous_variables=batch.num_exogenous_variables,
            )
            samples = None

        return Forecast(mean=mean, samples=samples)

    def assert_ev_compatibility(
        self,
        inputs,
        future_exogenous_variables,
        prediction_length,
        num_exogenous_variables,
    ) -> None:
        assert future_exogenous_variables.shape[-1] == prediction_length
        assert future_exogenous_variables.shape[0] == inputs.shape[0]
        assert num_exogenous_variables == future_exogenous_variables.shape[-2]

    def round_ft_ev(self, future_exogenous_variables, T_rounded):
        B, V_ev, T_future = future_exogenous_variables.shape
        dtype = future_exogenous_variables.dtype
        device = future_exogenous_variables.device
        padding = torch.zeros(B, V_ev, T_rounded - T_future, device=device, dtype=dtype)
        return torch.cat([future_exogenous_variables, padding], dim=-1)

    @torch.no_grad()
    def generate_mean(
        self,
        inputs: Float[torch.Tensor, "batch variate time_steps"],
        prediction_length: int,
        timestamp_seconds: Int[torch.Tensor, "batch variate time_steps"],
        time_interval_seconds: Int[torch.Tensor, "batch variate"],
        input_padding_mask: (
            Bool[torch.Tensor, "batch variate time_steps"] | None
        ) = None,
        id_mask: Float[torch.Tensor, "batch #variate time_steps"] | None = None,
        use_kv_cache: bool = False,
        future_exogenous_variables=None,
        num_exogenous_variables: int = 0,
    ) -> Float[torch.Tensor, "batch variate time_steps"]:
        if input_padding_mask is None:
            input_padding_mask = torch.ones_like(
                inputs, dtype=torch.bool, device=inputs.device
            )
        if id_mask is None:
            id_mask = torch.zeros_like(inputs, dtype=torch.int, device=inputs.device)

        if future_exogenous_variables is not None:
            self.assert_ev_compatibility(
                inputs,
                future_exogenous_variables,
                prediction_length,
                num_exogenous_variables,
            )

        patch_size = self.model.patch_embed.stride
        rounded_steps = int(np.ceil(prediction_length / patch_size) * patch_size)
        if rounded_steps > prediction_length and future_exogenous_variables is not None:
            future_exogenous_variables = self.round_ft_ev(
                future_exogenous_variables, rounded_steps
            )
        start_index = inputs.shape[-1]
        end_index = start_index + prediction_length

        dummy_padding = torch.ones(
            (input_padding_mask.shape[0], input_padding_mask.shape[1], patch_size),
            device=inputs.device,
            dtype=torch.bool,
        )
        dummy_id_mask = repeat(
            id_mask[:, :, -1:],
            "batch variates 1 -> batch variates patch_size",
            patch_size=patch_size,
        )
        if use_kv_cache:
            kv_cache = self.model.allocate_kv_cache(
                batch_size=inputs.shape[0],
                num_variates=inputs.shape[1],
                max_time_steps=inputs.shape[2] + rounded_steps,
                device=inputs.device,
                dtype=inputs.dtype,
            )
        else:
            kv_cache = None

        scaling_prefix_length = inputs.shape[-1]

        for idx in range(rounded_steps // patch_size):
            base_distr, loc, scale = self.model(
                inputs=inputs,
                input_padding_mask=input_padding_mask,
                id_mask=id_mask,
                kv_cache=kv_cache,
                scaling_prefix_length=scaling_prefix_length,
                num_exogenous_variables=num_exogenous_variables,
            )
            distr = self.create_affine_transformed(base_distr, loc, scale)

            samples = replace_extreme_values(distr.mean[:, :, -patch_size:])

            if future_exogenous_variables is not None:
                start, stop = idx * patch_size, (idx + 1) * patch_size
                samples[:, -num_exogenous_variables:] = future_exogenous_variables[
                    :, :, start:stop
                ]

            inputs = torch.cat([inputs, samples], dim=-1)
            id_mask = torch.cat([id_mask, dummy_id_mask], dim=-1)
            input_padding_mask = torch.cat([input_padding_mask, dummy_padding], dim=-1)
            for _ in range(patch_size):
                next_timestamp = timestamp_seconds[:, :, -1] + time_interval_seconds
                timestamp_seconds = torch.cat(
                    [timestamp_seconds, next_timestamp.unsqueeze(-1)], dim=-1
                )

        return inputs.detach()[:, :, start_index:end_index]

    @torch.no_grad()
    def generate_samples(
        self,
        inputs: Float[torch.Tensor, "batch variate time_steps"],
        prediction_length: int,
        num_samples: int,
        timestamp_seconds: Int[torch.Tensor, "batch variate time_steps"],
        time_interval_seconds: Int[torch.Tensor, "batch variate"],
        input_padding_mask: (
            Bool[torch.Tensor, "batch variate time_steps"] | None
        ) = None,
        id_mask: Float[torch.Tensor, "batch #variate time_steps"] | None = None,
        sampling_batch_size: int = 10,
        use_kv_cache: bool = False,
        future_exogenous_variables=None,
        num_exogenous_variables: int = 0,
    ) -> Float[torch.Tensor, "batch variate time_steps samples"]:
        if input_padding_mask is None:
            input_padding_mask = torch.ones_like(
                inputs, dtype=torch.bool, device=inputs.device
            )
        if id_mask is None:
            id_mask = torch.zeros_like(inputs, dtype=torch.int, device=inputs.device)

        if future_exogenous_variables is not None:
            self.assert_ev_compatibility(
                inputs,
                future_exogenous_variables,
                prediction_length,
                num_exogenous_variables,
            )

        assert (
            num_samples % sampling_batch_size == 0
        ), "num_samples must be divisible by sampling_batch_size"
        num_batches = num_samples // sampling_batch_size

        patch_size = self.model.patch_embed.patch_size
        rounded_steps = int(np.ceil(prediction_length / patch_size) * patch_size)
        if rounded_steps > prediction_length and future_exogenous_variables is not None:
            future_exogenous_variables = self.round_ft_ev(
                future_exogenous_variables, rounded_steps
            )
        start_index = inputs.shape[-1]
        end_index = start_index + prediction_length

        dummy_padding = torch.ones(
            (
                input_padding_mask.shape[0] * sampling_batch_size,
                input_padding_mask.shape[1],
                patch_size,
            ),
            dtype=torch.bool,
            device=inputs.device,
        )
        dummy_id_mask = repeat(
            id_mask[:, :, -1:],
            "batch variates 1 -> (sampling_batch_size batch) variates patch_size",
            sampling_batch_size=sampling_batch_size,
            patch_size=patch_size,
        )
        inputs = repeat(
            inputs,
            "batch variates seq_len -> (sampling_batch_size batch) variates seq_len",
            sampling_batch_size=sampling_batch_size,
        )
        if future_exogenous_variables is not None:
            future_exogenous_variables = repeat(
                future_exogenous_variables,
                "batch exogenous_variables future_time_steps -> (sampling_batch_size batch) exogenous_variables future_time_steps",
                sampling_batch_size=sampling_batch_size,
            )
        input_padding_mask = repeat(
            input_padding_mask,
            "batch variates seq_len -> (sampling_batch_size batch) variates seq_len",
            sampling_batch_size=sampling_batch_size,
        )
        id_mask = repeat(
            id_mask,
            "batch variates seq_len -> (sampling_batch_size batch) variates seq_len",
            sampling_batch_size=sampling_batch_size,
        )
        timestamp_seconds = repeat(
            timestamp_seconds,
            "batch variates seq_len -> (sampling_batch_size batch) variates seq_len",
            sampling_batch_size=sampling_batch_size,
        )
        time_interval_seconds = repeat(
            time_interval_seconds,
            "batch variates -> (sampling_batch_size batch) variates",
            sampling_batch_size=sampling_batch_size,
        )

        all_samples = []
        if use_kv_cache:
            kv_cache = self.model.allocate_kv_cache(
                batch_size=inputs.shape[0],
                num_variates=inputs.shape[1],
                max_time_steps=inputs.shape[2] + rounded_steps,
                device=inputs.device,
                dtype=inputs.dtype,
            )
        else:
            kv_cache = None

        scaling_prefix_length = inputs.shape[-1]

        for _ in range(num_batches):
            batch_inputs = torch.clone(inputs)
            batch_input_padding_mask = torch.clone(input_padding_mask)
            batch_id_mask = torch.clone(id_mask)
            batch_timestamp_seconds = torch.clone(timestamp_seconds)

            for idx in range(rounded_steps // patch_size):
                base_distr, loc, scale = self.model(
                    inputs=batch_inputs,
                    input_padding_mask=batch_input_padding_mask,
                    id_mask=batch_id_mask,
                    kv_cache=kv_cache,
                    scaling_prefix_length=scaling_prefix_length,
                    num_exogenous_variables=num_exogenous_variables,
                )
                distr = self.create_affine_transformed(base_distr, loc, scale)

                sample = distr.sample()
                assert sample is not None

                samples = replace_extreme_values(sample[:, :, -patch_size:])

                if future_exogenous_variables is not None:
                    start, stop = idx * patch_size, (idx + 1) * patch_size
                    samples[:, -num_exogenous_variables:] = future_exogenous_variables[
                        :, :, start:stop
                    ]
                batch_inputs = torch.cat([batch_inputs, samples], dim=-1)
                batch_id_mask = torch.cat([batch_id_mask, dummy_id_mask], dim=-1)
                batch_input_padding_mask = torch.cat(
                    [batch_input_padding_mask, dummy_padding], dim=-1
                )
                for _ in range(patch_size):
                    next_timestamp = (
                        batch_timestamp_seconds[:, :, -1] + time_interval_seconds
                    )
                    batch_timestamp_seconds = torch.cat(
                        [batch_timestamp_seconds, next_timestamp.unsqueeze(-1)], dim=-1
                    )
            all_samples.append(batch_inputs)
            if kv_cache is not None:
                kv_cache.reset()

        outputs = torch.cat(all_samples, dim=0)
        unfolded_outputs = rearrange(
            outputs,
            "(samples batch) variates seq_len -> batch variates seq_len samples",
            samples=num_samples,
        ).detach()

        return unfolded_outputs[:, :, start_index:end_index, :]

    @staticmethod
    def create_affine_transformed(
        base_distr: Distribution, loc: torch.Tensor, scale: torch.Tensor
    ) -> Distribution:
        base_shape = base_distr.mean.shape
        base_time_dim = base_shape[-1]
        loc_time_dim = loc.shape[-1]

        if loc_time_dim == 1:
            return AffineTransformed(base_distr, loc=loc, scale=scale)

        return AffineTransformed(
            base_distr,
            loc=loc[:, :, -base_time_dim:],
            scale=scale[:, :, -base_time_dim:],
        )
