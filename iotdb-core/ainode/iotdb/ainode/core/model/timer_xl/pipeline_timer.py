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

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_info import ModelInfo

logger = Logger()


class TimerPipeline(ForecastPipeline):
    def __init__(self, model_info: ModelInfo, **model_kwargs):
        super().__init__(model_info, **model_kwargs)

    def preprocess(self, inputs, **infer_kwargs) -> torch.Tensor:
        """
        Preprocess the input data by converting it to a 2D tensor (Timer-XL only supports 2D inputs).

        Parameters:
            inputs (list): A list of dictionaries containing input data,
                           where each dictionary should include a "targets" key.
            **infer_kwargs: Additional keyword arguments passed to the method.

        Returns:
            torch.Tensor: A 2D tensor of shape [batch_size, input_length] after squeezing
                          the target_count dimension.

        Raises:
            InferenceModelInternalException: If the model receives more than one target variable
                                             (i.e., when inputs.shape[1] != 1).
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

        # stack the data and get a 3D-tensor:[batch_size, target_count(1), input_length]
        inputs = torch.stack([data["targets"] for data in inputs], dim=0)
        if inputs.shape[1] != 1:
            raise InferenceModelInternalException(
                f"Model {model_id} only supports univariate forecast, but receives {inputs.shape[1]} target variables."
            )
        inputs = inputs.squeeze(1)
        return inputs

    def forecast(self, inputs: torch.Tensor, **infer_kwargs) -> torch.Tensor:
        """
        Generate forecasted outputs using the model based on the provided inputs.

        Parameters:
            inputs (torch.Tensor): A 2D tensor of shape [batch_size, input_length].
            **infer_kwargs: Additional inference parameters:
                - output_length (int): The length of the forecast output (default: 96).
                - revin (bool): Whether to apply revin (default: True).

        Returns:
            torch.Tensor: A tensor containing the forecasted outputs, with shape [batch_size, output_length].
        """
        output_length = infer_kwargs.get("output_length", 96)
        revin = infer_kwargs.get("revin", True)

        outputs = self.model.generate(inputs, max_new_tokens=output_length, revin=revin)
        return outputs

    def postprocess(self, outputs: torch.Tensor, **infer_kwargs) -> list[torch.Tensor]:
        """
        Postprocess the model's output by expanding its dimensions to match the expected shape.

        Parameters:
            outputs (torch.Tensor): The raw output 2D-tensor from the model with shape [batch_size, output_length].
            **infer_kwargs: Additional inference parameters passed to the method.

        Returns:
            list of torch.Tensor: A list of 2D tensors with shape [target_count(1), output_length].
        """
        outputs = [outputs[i].unsqueeze(0) for i in range(outputs.size(0))]
        outputs = super().postprocess(outputs, **infer_kwargs)
        return outputs
