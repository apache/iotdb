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

import logging
import warnings

import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline

logger = logging.getLogger(__name__)

_TOTO_INSTALL_MSG = (
    "toto-ts is required to use the Toto model but is not installed.\n"
    "Install it with:  pip install toto-ts\n"
    "Note: toto-ts pins specific versions of torch, numpy, and transformers "
    "that may conflict with other AINode dependencies. Install in a separate "
    "environment if needed."
)


def _import_toto():
    try:
        from toto.data.util.dataset import MaskedTimeseries
        from toto.inference.forecaster import TotoForecaster

        return MaskedTimeseries, TotoForecaster
    except ImportError as e:
        raise ImportError(_TOTO_INSTALL_MSG) from e


class TotoPipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)
        _, TotoForecaster = _import_toto()
        self.forecaster = TotoForecaster(self.model.backbone)

    def preprocess(self, inputs, **infer_kwargs):
        super().preprocess(inputs, **infer_kwargs)
        MaskedTimeseries, _ = _import_toto()
        processed_inputs = []

        for item in inputs:
            targets = item["targets"]
            if targets.ndim == 1:
                targets = targets.unsqueeze(0)

            n_variates, series_len = targets.shape
            device = targets.device

            if "past_covariates" in item or "future_covariates" in item:
                warnings.warn(
                    "TotoPipeline does not support covariates; they will be ignored.",
                    UserWarning,
                    stacklevel=2,
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

    def forecast(self, inputs, **infer_kwargs):
        output_length = infer_kwargs.get("output_length", 96)
        num_samples = infer_kwargs.get("num_samples", None)
        samples_per_batch = infer_kwargs.get("samples_per_batch", 10)

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
            result = self.forecaster.forecast(
                masked_ts,
                prediction_length=output_length,
                num_samples=num_samples,
                samples_per_batch=samples_per_batch,
            )
            mean = result.mean
            if mean.ndim == 3:
                mean = mean.mean(dim=0)
            if mean.ndim == 3 and mean.shape[0] == 1:
                mean = mean.squeeze(0)
            outputs.append(mean)
        return outputs

    def postprocess(self, outputs, **infer_kwargs):
        return super().postprocess(outputs, **infer_kwargs)
