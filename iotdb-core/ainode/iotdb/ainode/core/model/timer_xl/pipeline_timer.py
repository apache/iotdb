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


class TimerPipeline(ForecastPipeline):
    def __init__(self, model_info, **model_kwargs):
        super().__init__(model_info, model_kwargs=model_kwargs)

    def preprocess(self, inputs):
        """
        The inputs shape should be 3D, but Timer-XL only supports 2D tensor: [batch_size, sequence_length],
        we need to squeeze the target_count dimension.
        """
        inputs = super().preprocess(inputs)
        if inputs.shape[1] != 1:
            raise InferenceModelInternalException(
                f"[Inference] Model timer_xl only supports univarate forecast, but receives {inputs.shape[1]} target variables."
            )
        inputs = inputs.squeeze(1)
        return inputs

    def forecast(self, inputs, **infer_kwargs):
        predict_length = infer_kwargs.get("predict_length", 96)
        revin = infer_kwargs.get("revin", True)

        outputs = self.model.generate(
            inputs, max_new_tokens=predict_length, revin=revin
        )
        return outputs

    def postprocess(self, outputs: torch.Tensor):
        """
        The outputs shape should be 3D, so we need to expand dims.
        """
        outputs = super().postprocess(outputs.unsqueeze(1))
        return outputs
