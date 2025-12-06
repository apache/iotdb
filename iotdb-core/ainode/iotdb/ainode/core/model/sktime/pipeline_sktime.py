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

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline


class SktimePipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        model_kwargs.pop("device", None)  # sktime models run on CPU
        super().__init__(model_info, model_kwargs=model_kwargs)

    def _preprocess(self, inputs):
        return inputs

    def forecast(self, inputs, **infer_kwargs):
        predict_length = infer_kwargs.get("predict_length", 96)
        input_ids = self._preprocess(inputs)

        # Convert to pandas Series for sktime (sktime expects Series or DataFrame)
        # Handle batch dimension: if batch_size > 1, process each sample separately
        if len(input_ids.shape) == 2 and input_ids.shape[0] > 1:
            # Batch processing: convert each row to Series
            outputs = []
            for i in range(input_ids.shape[0]):
                series = pd.Series(
                    input_ids[i].cpu().numpy()
                    if isinstance(input_ids, torch.Tensor)
                    else input_ids[i]
                )
                output = self.model.generate(series, predict_length=predict_length)
                outputs.append(output)
            output = np.array(outputs)
        else:
            # Single sample: convert to Series
            if isinstance(input_ids, torch.Tensor):
                series = pd.Series(input_ids.squeeze().cpu().numpy())
            else:
                series = pd.Series(input_ids.squeeze())
            output = self.model.generate(series, predict_length=predict_length)
            # Add batch dimension if needed
            if len(output.shape) == 1:
                output = output[np.newaxis, :]

        return self._postprocess(output)

    def _postprocess(self, output):
        if isinstance(output, np.ndarray):
            return torch.from_numpy(output).float()
        return output
