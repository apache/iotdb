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
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.toto.data.util.dataset import MaskedTimeseries
from iotdb.ainode.core.model.toto.inference.forecaster import TotoForecaster

logger = Logger()


class TotoPipeline(ForecastPipeline):
    """
    Inference pipeline for the Toto time series foundation model.

    Converts raw input tensors into ``MaskedTimeseries`` objects and delegates
    autoregressive decoding to ``TotoForecaster``.  The forecaster is created
    lazily on the first call to ``forecast()`` so that pipeline construction
    does not require a live model (useful during import / registration time).
    """

    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)
        # Forecaster is created lazily to avoid issues at construction time.
        self._forecaster: TotoForecaster | None = None

    def _get_forecaster(self) -> TotoForecaster:
        """Return the cached forecaster, creating it on first call."""
        if self._forecaster is None:
            self._forecaster = TotoForecaster(self.model.backbone)
        return self._forecaster

    def _preprocess(self, inputs, **infer_kwargs):
        """
        Preprocess input data for Toto.

        Converts each input dict into a ``MaskedTimeseries`` named-tuple that
        the ``TotoForecaster`` expects.

        Parameters
        ----------
        inputs : list of dict
            A list of dictionaries containing input data. Each dictionary contains:
            - 'targets': A tensor (1D or 2D) of shape (input_length,) or (target_count, input_length).

        infer_kwargs: Additional keyword arguments for inference, such as:
            - `output_length`(int): Prediction length.

        Returns
        -------
        list of MaskedTimeseries
            Processed inputs compatible with Toto's forecaster.
        """
        processed_inputs = []
        for item in inputs:
            targets = item["targets"]
            if targets.ndim == 1:
                targets = targets.unsqueeze(0)

            n_variates, series_len = targets.shape
            device = targets.device

            if "past_covariates" in item or "future_covariates" in item:
                logger.warning(
                    "TotoPipeline does not support covariates; they will be ignored."
                )

            padding_mask = ~torch.isnan(targets)
            targets = targets.nan_to_num(0.0)

            id_mask = torch.zeros(
                n_variates, series_len, dtype=torch.long, device=device
            )
            timestamp_seconds = (
                torch.arange(series_len, dtype=torch.long, device=device)
                .unsqueeze(0)
                .expand(n_variates, series_len)
            )
            time_interval_seconds = torch.ones(
                n_variates, dtype=torch.long, device=device
            )

            processed_inputs.append(
                MaskedTimeseries(
                    series=targets,
                    padding_mask=padding_mask,
                    id_mask=id_mask,
                    timestamp_seconds=timestamp_seconds,
                    time_interval_seconds=time_interval_seconds,
                )
            )

        return processed_inputs

    def forecast(self, inputs, **infer_kwargs) -> list[torch.Tensor]:
        output_length = infer_kwargs.get("output_length", 96)
        num_samples = infer_kwargs.get("num_samples", None)
        samples_per_batch = infer_kwargs.get("samples_per_batch", 10)

        forecaster = self._get_forecaster()

        outputs = []
        for masked_ts in inputs:
            masked_ts = masked_ts._replace(
                series=masked_ts.series.to(self.model.device),
                padding_mask=masked_ts.padding_mask.to(self.model.device),
                id_mask=masked_ts.id_mask.to(self.model.device),
                timestamp_seconds=masked_ts.timestamp_seconds.to(self.model.device),
                time_interval_seconds=masked_ts.time_interval_seconds.to(
                    self.model.device
                ),
            )
            result = forecaster.forecast(
                masked_ts,
                prediction_length=output_length,
                num_samples=num_samples,
                samples_per_batch=samples_per_batch,
            )
            mean = result.mean
            # Remove batch dimension if present (batch=1 squeeze).
            if mean.ndim == 3 and mean.shape[0] == 1:
                mean = mean.squeeze(0)
            outputs.append(mean)
        return outputs

    def _postprocess(self, outputs, **infer_kwargs) -> list[torch.Tensor]:
        return outputs
