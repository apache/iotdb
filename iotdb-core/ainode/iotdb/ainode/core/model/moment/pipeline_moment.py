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

# This file contains code adapted from the MOMENT project
# (https://github.com/moment-timeseries-foundation-model/moment),
# originally licensed under the MIT License.

import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_info import ModelInfo

logger = Logger()

# MOMENT requires a fixed input length of 512 timesteps
MOMENT_SEQ_LEN = 512


class MomentPipeline(ForecastPipeline):
    """
    Inference pipeline for the MOMENT time series foundation model.

    MOMENT processes fixed-length (512) univariate patches through a T5 encoder
    and produces forecasts via a single-shot linear head. Each channel/variate
    is processed independently (channel-independent design).

    The pipeline handles:
    - Padding/truncating inputs to the required 512 length
    - Constructing input masks for padded positions
    - Iterative forecasting for horizons beyond the model's native capacity
    """

    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def _preprocess(self, inputs, **infer_kwargs) -> dict:
        """
        Preprocess input data for MOMENT.

        Converts the list of input dicts into a single tensor padded/truncated
        to MOMENT's required sequence length of 512. Also constructs an
        input_mask indicating which timesteps are observed vs padded.

        Parameters
        ----------
        inputs : list of dict
            Each dict has key ``"targets"`` with a tensor of shape
            ``(target_count, input_length)``.

        Returns
        -------
        dict
            ``"x_enc"``: tensor of shape ``[batch, n_channels, 512]``
            ``"input_mask"``: tensor of shape ``[batch, 512]``
        """
        if inputs[0].get("past_covariates") or inputs[0].get("future_covariates"):
            logger.warning(
                "MomentPipeline does not support covariates; they will be ignored."
            )

        batch_tensors = []
        batch_masks = []

        for item in inputs:
            targets = item["targets"]  # [target_count, input_length]
            if targets.ndim == 1:
                targets = targets.unsqueeze(0)

            n_channels, input_length = targets.shape

            if input_length >= MOMENT_SEQ_LEN:
                # Truncate: take the last MOMENT_SEQ_LEN timesteps
                x = targets[:, -MOMENT_SEQ_LEN:]
                mask = torch.ones(MOMENT_SEQ_LEN, device=targets.device)
            else:
                # Left-pad with zeros
                pad_len = MOMENT_SEQ_LEN - input_length
                x = torch.nn.functional.pad(targets, (pad_len, 0), value=0.0)
                mask = torch.cat(
                    [
                        torch.zeros(pad_len, device=targets.device),
                        torch.ones(input_length, device=targets.device),
                    ]
                )

            batch_tensors.append(x)
            batch_masks.append(mask)

        x_enc = torch.stack(batch_tensors, dim=0)  # [batch, n_channels, 512]
        input_mask = torch.stack(batch_masks, dim=0)  # [batch, 512]

        return {"x_enc": x_enc, "input_mask": input_mask}

    def forecast(self, inputs: dict, **infer_kwargs) -> list[torch.Tensor]:
        """
        Run MOMENT forecasting inference.

        For output_length <= model forecast_horizon, a single forward pass
        suffices. For longer horizons, iterative (autoregressive) forecasting
        is used: each step's predictions are appended to the context window
        and fed back as input.

        Parameters
        ----------
        inputs : dict
            Contains ``"x_enc"`` and ``"input_mask"`` from _preprocess.
        infer_kwargs : dict
            ``output_length`` (int): desired forecast length, default 96.

        Returns
        -------
        list of torch.Tensor
            Each tensor has shape ``[n_channels, output_length]``.
        """
        output_length = infer_kwargs.get("output_length", 96)
        x_enc = inputs["x_enc"].to(self.model.device)
        input_mask = inputs["input_mask"].to(self.model.device)

        model_horizon = self.model.config.forecast_horizon
        batch_size, n_channels, seq_len = x_enc.shape

        if output_length <= model_horizon:
            # Single-shot inference
            with torch.no_grad():
                output = self.model(x_enc=x_enc, input_mask=input_mask)
            forecasts = output.forecast[:, :, :output_length]
        else:
            # Iterative forecasting for long horizons
            forecasts_list = []
            remaining = output_length
            current_x = x_enc
            current_mask = input_mask

            while remaining > 0:
                with torch.no_grad():
                    output = self.model(x_enc=current_x, input_mask=current_mask)
                step_forecast = output.forecast[:, :, : min(model_horizon, remaining)]
                forecasts_list.append(step_forecast)
                remaining -= step_forecast.shape[-1]

                if remaining > 0:
                    # Slide context window: append forecast, drop oldest
                    step_len = step_forecast.shape[-1]
                    current_x = torch.cat(
                        [current_x[:, :, step_len:], step_forecast], dim=-1
                    )
                    current_mask = torch.ones(batch_size, seq_len, device=x_enc.device)

            forecasts = torch.cat(forecasts_list, dim=-1)

        # Split batch into list of per-sample tensors
        return [forecasts[i] for i in range(batch_size)]

    def _postprocess(
        self, outputs: list[torch.Tensor], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        Postprocess outputs. Each tensor is already [n_channels, output_length].
        """
        return outputs
