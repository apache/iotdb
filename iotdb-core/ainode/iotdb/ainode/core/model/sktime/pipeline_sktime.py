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
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_info import ModelInfo

logger = Logger()


class SktimePipeline(ForecastPipeline):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        model_kwargs.pop("device", None)  # sktime models run on CPU
        super().__init__(model_info, **model_kwargs)

    def preprocess(
        self,
        inputs: list[dict[str, dict[str, torch.Tensor] | torch.Tensor]],
        **infer_kwargs,
    ) -> list[pd.Series]:
        """
        Preprocess the input data for forecasting.

        Parameters:
            inputs (list): A list of dictionaries containing input data with key 'targets'.

        Returns:
            list of pd.Series: Processed inputs for the model with each of shape [input_length, ].
        """
        model_id = self.model_info.model_id

        inputs = super().preprocess(inputs, **infer_kwargs)

        # Here, we assume element in list has same history_length,
        # otherwise, the model cannot proceed
        if inputs[0].get("past_covariates", None) or inputs[0].get(
            "future_covariates", None
        ):
            logger.warning(
                f"[Inference] Past_covariates and future_covariates will be ignored, as they are not supported for model {model_id}."
            )

        # stack the data and get a 3D-tensor: [batch_size, target_count(1), input_length]
        inputs = torch.stack([data["targets"] for data in inputs], dim=0)
        if inputs.shape[1] != 1:
            raise InferenceModelInternalException(
                f"Model {model_id} only supports univariate forecast, but receives {inputs.shape[1]} target variables."
            )
        # Transform into a 2D-tensor: [batch_size, input_length]
        inputs = inputs.squeeze(1)
        # Transform into a list of Series with each of shape [input_length,]
        inputs = [pd.Series(data.cpu().numpy()) for i, data in enumerate(inputs)]

        return inputs

    def forecast(self, inputs: list[pd.Series], **infer_kwargs) -> np.ndarray:
        """
        Generate forecasts from the model for given inputs.

        Parameters:
            inputs (list[Series]): A list of input data for forecasting with each of shape [input_length,].
            **infer_kwargs: Additional inference parameters such as:
                - 'output_length'(int): The number of time points that model should generate.

        Returns:
            np.ndarray: Forecasted outputs.
        """
        output_length = infer_kwargs.get("output_length", 96)

        # Batch processing
        outputs = []
        for series in inputs:
            output = self.model.generate(series, output_length=output_length)
            outputs.append(output)
        outputs = np.array(outputs)

        return outputs

    def postprocess(self, outputs: np.ndarray, **infer_kwargs) -> list[torch.Tensor]:
        """
        Postprocess the model's outputs.

        Parameters:
            outputs (np.ndarray): Model output to be processed.
            **infer_kwargs: Additional inference parameters.

        Returns:
            list of torch.Tensor: List of 2D-tensors with shape [target_count(1), output_length].
        """

        # Transform outputs into a 2D-tensor: [batch_size, output_length]
        outputs = torch.from_numpy(outputs).float()
        outputs = [outputs[i].unsqueeze(0) for i in range(outputs.size(0))]
        outputs = super().postprocess(outputs, **infer_kwargs)
        return outputs
