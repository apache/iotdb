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

import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.timestral.forecaster import TimestralForecaster


class TimestralPipeline(ForecastPipeline):
    """
    Forecasting pipeline of Timestral.

    Each input is one series of one or more targets, forecast jointly, with optional past
    covariates and future-known covariates. The model predicts 21 quantiles (0.01 to 0.99);
    horizons longer than one decode block (1024 steps) are forecast block by block, feeding
    each block's median back as context. The pipeline returns the median.
    """

    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)
        self.forecaster = TimestralForecaster(self.model)

    def _preprocess(self, inputs, **infer_kwargs) -> list[dict]:
        """
        Convert the validated inputs to the arguments of the forecaster.

        Parameters
        ----------
        inputs : list of dict
            Each dictionary contains:
            - 'targets': A tensor of shape (input_length,) or (target_count, input_length).
            - 'past_covariates' (optional): A dictionary of tensors of shape (input_length,).
            - 'future_covariates' (optional): A dictionary of tensors of shape (output_length,),
              whose keys are a subset of 'past_covariates'.

        Returns
        -------
        list of dict
            One dictionary per input, with:
            - 'context': The targets, a (1, target_count, input_length) tensor.
            - 'past_covariates': The covariates known only in the past, a
              (1, count, input_length) tensor, or None.
            - 'known_covariates': The covariates also known in the future, a
              (1, count, input_length + output_length) tensor of their past and future
              values, or None.
        """
        series = []
        for item in inputs:
            targets = item["targets"]
            if targets.ndim == 1:
                targets = targets.unsqueeze(0)
            past_covariates = item.get("past_covariates") or {}
            future_covariates = item.get("future_covariates") or {}
            past_only = [k for k in past_covariates if k not in future_covariates]
            known = [k for k in past_covariates if k in future_covariates]
            series.append(
                {
                    "context": targets.float().unsqueeze(0),
                    "past_covariates": (
                        torch.stack([past_covariates[k] for k in past_only])
                        .float()
                        .unsqueeze(0)
                        if past_only
                        else None
                    ),
                    "known_covariates": (
                        torch.stack(
                            [
                                torch.cat([past_covariates[k], future_covariates[k]])
                                for k in known
                            ]
                        )
                        .float()
                        .unsqueeze(0)
                        if known
                        else None
                    ),
                }
            )
        return series

    def forecast(self, inputs: list[dict], **infer_kwargs) -> list[torch.Tensor]:
        """
        Forecast the series, batching those of the same shape without covariates.

        Returns
        -------
        list of torch.Tensor
            One tensor per series, of shape (target_count, output_length, quantile_count).
        """
        output_length = infer_kwargs.get("output_length", 96)
        outputs: list[torch.Tensor | None] = [None] * len(inputs)
        batches: dict[tuple, list[int]] = {}
        for i, item in enumerate(inputs):
            if item["past_covariates"] is None and item["known_covariates"] is None:
                batches.setdefault(tuple(item["context"].shape), []).append(i)
            else:
                batches[("series", i)] = [i]
        for indices in batches.values():
            items = [inputs[i] for i in indices]
            forecast = self.forecaster.forecast(
                torch.cat([item["context"] for item in items]),
                output_length,
                past_covariates=items[0]["past_covariates"],
                known_covariates=items[0]["known_covariates"],
            )
            for i, quantiles in zip(indices, forecast.quantiles):
                outputs[i] = quantiles
        return outputs

    def _postprocess(
        self, outputs: list[torch.Tensor], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        Select the median of each forecast.

        Returns
        -------
        list of torch.Tensor
            One tensor per series, of shape (target_count, output_length).
        """
        median = self.forecaster.median_index
        return [output[..., median].float().cpu().clone() for output in outputs]
