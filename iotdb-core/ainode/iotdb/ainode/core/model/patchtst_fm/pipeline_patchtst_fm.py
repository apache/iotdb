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


class PatchTSTFMPipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **infer_kwargs):
        inputs = super().preprocess(inputs, **infer_kwargs)
        for idx, item in enumerate(inputs):
            # Model expects float32
            target_tensor = item["targets"].to(torch.float32)

            # Expand 1D tensor [length] to [batch=1, length]
            if target_tensor.ndim == 1:
                target_tensor = target_tensor.unsqueeze(0)

            item["targets"] = target_tensor
        return inputs

    def forecast(self, inputs, **infer_kwargs) -> list[torch.Tensor]:
        """
        Run the PatchTST-FM-R1 forward pass for each input in the batch.
        The model expects a list of 1D tensors (one per variate) and returns
        a PatchTSTFMPredictionOutput with a `quantile_predictions` attribute.
        """
        forecasts = []
        for item in inputs:
            targets = item["targets"]
            pred_length = infer_kwargs.get("output_length", 96)
            # Move to device and convert [n_variates, length] → list of 1D tensors
            # as required by PatchTSTFMForPrediction.forward()
            tensor = targets.to(self.device)
            tensor_list = [tensor[i] for i in range(tensor.shape[0])]
            with torch.no_grad():
                output = self.model(inputs=tensor_list, prediction_length=pred_length)
            forecasts.append(output.quantile_predictions)
        return forecasts

    def postprocess(
        self, outputs: list[torch.Tensor], **infer_kwargs
    ) -> list[torch.Tensor]:
        """
        The IBM Model returns quantiles [batch, variates, prediction_length, quantiles].
        We reduce this to [variates, prediction_length] by taking the median or mean.
        """
        final_outputs = []
        for output in outputs:
            # Remove batch dimension if it is just a single batch
            if output.ndim == 4:
                output = output.squeeze(0)

            # Average out the quantiles to get a point forecast
            point_forecast = output.mean(dim=-1)
            final_outputs.append(point_forecast)

        return super().postprocess(final_outputs, **infer_kwargs)
