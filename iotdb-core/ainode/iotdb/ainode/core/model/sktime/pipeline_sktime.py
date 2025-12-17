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

import numpy as np
import pandas as pd
import torch

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline


class SktimePipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        model_kwargs.pop("device", None)  # sktime models run on CPU
        super().__init__(model_info, model_kwargs=model_kwargs)

    def preprocess(self, inputs):
        inputs = super().preprocess(inputs)
        if inputs.shape[1] != 1:
            raise InferenceModelInternalException(
                f"[Inference] Sktime model only supports univarate forecast, but receives {inputs.shape[1]} target variables."
            )
        inputs = inputs.squeeze(1)
        return inputs

    def forecast(self, inputs, **infer_kwargs):
        predict_length = infer_kwargs.get("predict_length", 96)

        # Convert to pandas Series for sktime (sktime expects Series or DataFrame)
        # Handle batch dimension: if batch_size > 1, process each sample separately
        if len(inputs.shape) == 2 and inputs.shape[0] > 1:
            # Batch processing: convert each row to Series
            outputs = []
            for i in range(inputs.shape[0]):
                series = pd.Series(
                    inputs[i].cpu().numpy()
                    if isinstance(inputs, torch.Tensor)
                    else inputs[i]
                )
                output = self.model.generate(series, predict_length=predict_length)
                outputs.append(output)
            outputs = np.array(outputs)
        else:
            # Single sample: convert to Series
            if isinstance(inputs, torch.Tensor):
                series = pd.Series(inputs.squeeze().cpu().numpy())
            else:
                series = pd.Series(inputs.squeeze())
            outputs = self.model.generate(series, predict_length=predict_length)
            # Add batch dimension if needed
            if len(outputs.shape) == 1:
                outputs = outputs[np.newaxis, :]

        return outputs

    def postprocess(self, outputs):
        if isinstance(outputs, np.ndarray):
            outputs = torch.from_numpy(outputs).float()
        outputs = super().postprocess(outputs.unsqueeze(1))
        return outputs
