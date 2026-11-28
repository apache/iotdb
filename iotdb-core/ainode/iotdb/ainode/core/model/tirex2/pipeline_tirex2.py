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
from iotdb.ainode.core.model.tirex2.types import TimeseriesType


class TiRex2Pipeline(ForecastPipeline):
    """
    Forecasting pipeline of TiRex-2.

    Each input is one series of one or more targets, forecast jointly, with optional past
    covariates and future-known covariates. The model predicts nine quantiles (0.1 to 0.9) of up
    to 320 future steps in one pass; the pipeline returns the median.
    """

    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    @property
    def max_output_length(self) -> int:
        return self.model.future_len

    def _preprocess(self, inputs, **infer_kwargs) -> list[TimeseriesType]:
        """
        Convert the validated inputs to TiRex-2 series.

        Parameters
        ----------
        inputs : list of dict
            Each dictionary contains:
            - 'targets': A tensor of shape (input_length,) or (target_count, input_length).
            - 'past_covariates' (optional): A dictionary of tensors of shape (input_length,).
            - 'future_covariates' (optional): A dictionary of tensors of shape (output_length,),
              whose keys are a subset of 'past_covariates'.

        infer_kwargs:
            - `output_length` (int): The number of steps to forecast, at most 320.

        Returns
        -------
        list of TimeseriesType
            One series per input: the targets as a (target_count, input_length) tensor, the
            covariates known only in the past as a (count, input_length) tensor, and the
            covariates also known in the future as a (count, input_length + output_length)
            tensor of their past and future values.
        """
        output_length = infer_kwargs.get("output_length", 96)
        if output_length > self.max_output_length:
            raise ValueError(
                f"TiRex-2 forecasts at most {self.max_output_length} steps, "
                f"but output_length is {output_length}."
            )
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
                TimeseriesType(
                    target=targets.float(),
                    past_covariates=(
                        torch.stack([past_covariates[k] for k in past_only]).float()
                        if past_only
                        else None
                    ),
                    future_covariates=(
                        torch.stack(
                            [
                                torch.cat([past_covariates[k], future_covariates[k]])
                                for k in known
                            ]
                        ).float()
                        if known
                        else None
                    ),
                )
            )
        return series

    def forecast(
        self, inputs: list[TimeseriesType], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        Forecast all series in one batch.

        Returns
        -------
        list of torch.Tensor
            One tensor per series, of shape (target_count, quantile_count, output_length).
        """
        output_length = infer_kwargs.get("output_length", 96)
        with torch.no_grad():
            return self.model.predict(inputs, output_length)

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
        median = self.model.quantiles.tolist().index(0.5)
        return [output[:, median, :].float().cpu() for output in outputs]
