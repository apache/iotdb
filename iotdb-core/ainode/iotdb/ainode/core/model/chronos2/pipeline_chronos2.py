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
from einops import rearrange, repeat
from torch.utils.data import DataLoader

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.chronos2.dataset import Chronos2Dataset, DatasetMode
from iotdb.ainode.core.model.chronos2.utils import (
    interpolate_quantiles,
    weighted_quantile,
)

logger = Logger()


class Chronos2Pipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, model_kwargs=model_kwargs)

    def preprocess(self, inputs):
        inputs = super().preprocess(inputs)
        return inputs

    @property
    def model_context_length(self) -> int:
        return self.model.chronos_config.context_length

    @property
    def model_output_patch_size(self) -> int:
        return self.model.chronos_config.output_patch_size

    @property
    def model_prediction_length(self) -> int:
        return (
            self.model.chronos_config.max_output_patches
            * self.model.chronos_config.output_patch_size
        )

    @property
    def quantiles(self) -> list[float]:
        return self.model.chronos_config.quantiles

    @property
    def max_output_patches(self) -> int:
        return self.model.chronos_config.max_output_patches

    @staticmethod
    def _slide_context_and_future_covariates(
        context: torch.Tensor, future_covariates: torch.Tensor, slide_by: int
    ) -> tuple[torch.Tensor, torch.Tensor]:
        # replace context with future_covariates, where the values of future covariates are known (not NaN)
        future_covariates_slice = future_covariates[..., :slide_by]
        context[..., -slide_by:] = torch.where(
            torch.isnan(future_covariates_slice),
            context[..., -slide_by:],
            future_covariates_slice,
        )
        # shift future_covariates
        future_covariates = future_covariates[..., slide_by:]

        return context, future_covariates

    @staticmethod
    def _get_prob_mass_per_quantile_level(
        quantile_levels: torch.Tensor,
    ) -> torch.Tensor:
        """
        Computes normalized probability masses for quantile levels using trapezoidal rule approximation.

        Each quantile receives probability mass proportional to the width of its surrounding interval,
        creating a piecewise uniform distribution. The mass for quantile q_i is computed as
        (q_{i+1} - q_{i-1}) / 2, where q_0 = 0 and q_{n+1} = 1.

        Parameters
        ----------
        quantile_levels : torch.Tensor
            The quantile levels, must be strictly in (0, 1)

        Returns
        -------
        torch.Tensor
            The normalized probability mass per quantile
        """
        assert quantile_levels.ndim == 1
        assert quantile_levels.min() > 0.0 and quantile_levels.max() < 1.0

        device = quantile_levels.device
        boundaries = torch.cat(
            [
                torch.tensor([0.0], device=device),
                quantile_levels,
                torch.tensor([1.0], device=device),
            ]
        )
        prob_mass = (boundaries[2:] - boundaries[:-2]) / 2
        return prob_mass / prob_mass.sum()

    def _prepare_inputs_for_long_horizon_unrolling(
        self,
        context: torch.Tensor,
        group_ids: torch.Tensor,
        future_covariates: torch.Tensor,
        unrolled_quantiles: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        # Expand the context, future_covariates and group_ids along a new "quantile" axis
        if future_covariates is not None:
            future_covariates = repeat(
                future_covariates, "b t -> b q t", q=len(unrolled_quantiles)
            )
        context = repeat(context, "b t -> b q t", q=len(unrolled_quantiles))
        group_ids = repeat(group_ids, "b -> b q", q=len(unrolled_quantiles))
        # Shift the group_ids so that mixing is enabled only for time series with the same group_id and
        # at the same quantile level, e.g., if the group_ids were [0, 0, 1, 1, 1] initially, after expansion
        # and shifting they will be:
        # [[0,  1,  2,  3,  4,  5,  6,  7,  8],
        #  [0,  1,  2,  3,  4,  5,  6,  7,  8],
        #  [9, 10, 11, 12, 13, 14, 15, 16, 17],
        #  [9, 10, 11, 12, 13, 14, 15, 16, 17],
        #  [9, 10, 11, 12, 13, 14, 15, 16, 17]]
        group_ids = group_ids * len(unrolled_quantiles) + torch.arange(
            len(unrolled_quantiles), device=self.model.device
        ).unsqueeze(0)
        # We unroll the quantiles in unrolled_quantiles to the future and each unrolled quantile gives
        # len(self.quantiles) predictions, so we end up with len(unrolled_quantiles) * len(self.quantiles)
        # "samples". unrolled_sample_weights specifies the amount of probability mass covered by each sample.
        # Note that this effectively leads to shrinking of the probability space but it is better heuristic
        # than just using the median to unroll, which leads to uncertainty collapse.
        unrolled_sample_weights = torch.outer(
            self._get_prob_mass_per_quantile_level(unrolled_quantiles),
            self._get_prob_mass_per_quantile_level(torch.tensor(self.quantiles)),
        )

        return context, group_ids, future_covariates, unrolled_sample_weights

    def _autoregressive_unroll_for_long_horizon(
        self,
        context: torch.Tensor,
        group_ids: torch.Tensor,
        future_covariates: torch.Tensor,
        prediction: torch.Tensor,
        unrolled_quantiles: torch.Tensor,
        unrolled_sample_weights: torch.Tensor,
        num_output_patches: int,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        # Get unrolled_quantiles from prediction and append it to the expanded context
        prediction_unrolled = interpolate_quantiles(
            query_quantile_levels=unrolled_quantiles,
            original_quantile_levels=self.quantiles,
            original_values=rearrange(prediction, "b q h -> b h q"),
        )
        prediction_unrolled = rearrange(prediction_unrolled, "b h q -> b q h")
        context = torch.cat([context, prediction_unrolled], dim=-1)[
            ..., -self.model_context_length :
        ]
        n_paths = len(unrolled_quantiles)

        # Shift future_covariates by prediction.shape[-1] while replacing the predicted values
        # of future covariates in the context with their actual values, if known
        if future_covariates is not None:
            context, future_covariates = self._slide_context_and_future_covariates(
                context=context,
                future_covariates=future_covariates,
                slide_by=prediction.shape[-1],
            )

        # Reshape (batch, n_paths, context_length) -> (batch * n_paths, context_length)
        prediction = self._predict_step(
            context=rearrange(context, "b n t -> (b n) t"),
            future_covariates=(
                rearrange(future_covariates, "b n t -> (b n) t")
                if future_covariates is not None
                else None
            ),
            group_ids=rearrange(group_ids, "b n -> (b n)"),
            num_output_patches=num_output_patches,
        )
        # Reshape predictions from (batch * n_paths, n_quantiles, length) to (batch, n_paths * n_quantiles, length)
        prediction = rearrange(prediction, "(b n) q h -> b (n q) h", n=n_paths)
        # Reduce `n_paths * n_quantiles` to n_quantiles and transpose back
        prediction = weighted_quantile(
            query_quantile_levels=self.quantiles,
            sample_weights=rearrange(unrolled_sample_weights, "n q -> (n q)"),
            samples=rearrange(prediction, "b (n q) h -> b h (n q)", n=n_paths),
        )
        prediction = rearrange(prediction, "b h q -> b q h")

        return prediction, context, future_covariates

    def forecast(self, inputs, **infer_kwargs):
        model_prediction_length = self.model_prediction_length
        prediction_length = infer_kwargs.get("predict_length", 96)
        # The maximum number of output patches to generate in a single forward pass before the long-horizon heuristic kicks in. Note: A value larger
        # than the model's default max_output_patches may lead to degradation in forecast accuracy, defaults to a model-specific value
        max_output_patches = infer_kwargs.get(
            "max_output_patches", self.max_output_patches
        )
        # The set of quantiles to use when making long-horizon predictions; must be a subset of the model's default quantiles. These quantiles
        # are appended to the historical context and input into the model autoregressively to generate long-horizon predictions. Note that the
        # effective batch size increases by a factor of `len(unrolled_quantiles)` when making long-horizon predictions,
        # by default [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        unrolled_quantiles = infer_kwargs.get(
            "unrolled_quantiles", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        )

        if not set(unrolled_quantiles).issubset(self.quantiles):
            raise ValueError(
                f"Unrolled quantiles must be a subset of the model's quantiles. "
                f"Found: {unrolled_quantiles=}, model_quantiles={self.quantiles}"
            )
        unrolled_quantiles_tensor = torch.tensor(unrolled_quantiles)

        if prediction_length > model_prediction_length:
            msg = (
                f"We recommend keeping prediction length <= {model_prediction_length}. "
                "The quality of longer predictions may degrade since the model is not optimized for it. "
            )
            logger.warning(msg)

        context_length = inputs.shape[-1]
        if context_length > self.model_context_length:
            logger.warning(
                f"The specified context_length {context_length} is greater than the model's default context length {self.model_context_length}. "
                f"Resetting context_length to {self.model_context_length}."
            )
            context_length = self.model_context_length

        test_dataset = Chronos2Dataset.convert_inputs(
            inputs=inputs,
            context_length=context_length,
            prediction_length=prediction_length,
            batch_size=256,
            output_patch_size=self.model_output_patch_size,
            mode=DatasetMode.TEST,
        )
        test_loader = DataLoader(
            test_dataset,
            batch_size=None,
            pin_memory=True,
            shuffle=False,
            drop_last=False,
        )

        all_predictions: list[torch.Tensor] = []
        for batch in test_loader:
            assert batch["future_target"] is None
            batch_context = batch["context"]
            batch_group_ids = batch["group_ids"]
            batch_future_covariates = batch["future_covariates"]
            batch_target_idx_ranges = batch["target_idx_ranges"]

            batch_prediction = self._predict_batch(
                context=batch_context,
                group_ids=batch_group_ids,
                future_covariates=batch_future_covariates,
                unrolled_quantiles_tensor=unrolled_quantiles_tensor,
                prediction_length=prediction_length,
                max_output_patches=max_output_patches,
                target_idx_ranges=batch_target_idx_ranges,
            )
            all_predictions.extend(batch_prediction)

        return all_predictions

    def _predict_batch(
        self,
        context: torch.Tensor,
        group_ids: torch.Tensor,
        future_covariates: torch.Tensor,
        unrolled_quantiles_tensor: torch.Tensor,
        prediction_length: int,
        max_output_patches: int,
        target_idx_ranges: list[tuple[int, int]],
    ) -> list[torch.Tensor]:
        context = context.to(device=self.model.device, dtype=torch.float32)
        group_ids = group_ids.to(device=self.model.device)
        future_covariates = future_covariates.to(
            device=self.model.device, dtype=torch.float32
        )

        def get_num_output_patches(remaining_horizon: int):
            num_output_patches = math.ceil(
                remaining_horizon / self.model_output_patch_size
            )
            num_output_patches = min(num_output_patches, max_output_patches)

            return num_output_patches

        predictions = []
        remaining = prediction_length

        # predict first set of patches up to max_output_patches
        prediction: torch.Tensor = self._predict_step(
            context=context,
            group_ids=group_ids,
            future_covariates=future_covariates,
            num_output_patches=get_num_output_patches(remaining),
        )
        predictions.append(prediction)
        remaining -= prediction.shape[-1]

        # prepare inputs for long horizon prediction
        if remaining > 0:
            context, group_ids, future_covariates, unrolled_sample_weights = (
                self._prepare_inputs_for_long_horizon_unrolling(
                    context=context,
                    group_ids=group_ids,
                    future_covariates=future_covariates,
                    unrolled_quantiles=unrolled_quantiles_tensor,
                )
            )

        # long horizon heuristic
        while remaining > 0:
            prediction, context, future_covariates = (
                self._autoregressive_unroll_for_long_horizon(
                    context=context,
                    group_ids=group_ids,
                    future_covariates=future_covariates,
                    prediction=prediction,
                    unrolled_quantiles=unrolled_quantiles_tensor,
                    unrolled_sample_weights=unrolled_sample_weights,
                    num_output_patches=get_num_output_patches(remaining),
                )
            )
            predictions.append(prediction)
            remaining -= prediction.shape[-1]

        batch_prediction = torch.cat(predictions, dim=-1)[..., :prediction_length].to(
            dtype=torch.float32, device="cpu"
        )

        return [batch_prediction[start:end] for (start, end) in target_idx_ranges]

    def _predict_step(
        self,
        context: torch.Tensor,
        group_ids: torch.Tensor,
        future_covariates: torch.Tensor | None,
        num_output_patches: int,
    ) -> torch.Tensor:
        kwargs = {}
        if future_covariates is not None:
            output_size = num_output_patches * self.model_output_patch_size

            if output_size > future_covariates.shape[1]:
                batch_size = len(future_covariates)
                padding_size = output_size - future_covariates.shape[1]
                padding_tensor = torch.full(
                    (batch_size, padding_size),
                    fill_value=torch.nan,
                    device=future_covariates.device,
                )
                future_covariates = torch.cat(
                    [future_covariates, padding_tensor], dim=1
                )

            else:
                future_covariates = future_covariates[..., :output_size]
            kwargs["future_covariates"] = future_covariates
        with torch.no_grad():
            prediction: torch.Tensor = self.model(
                context=context,
                group_ids=group_ids,
                num_output_patches=num_output_patches,
                **kwargs,
            ).quantile_preds.to(context)

        return prediction

    def postprocess(self, output: torch.Tensor):
        return output[0].mean(dim=1, keepdim=True)
