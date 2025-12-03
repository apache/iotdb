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

from abc import ABC

import torch

from iotdb.ainode.core.exception import InferenceModelInternalError
from iotdb.ainode.core.model.model_loader import load_model


class BasicPipeline(ABC):
    def __init__(self, model_info, **infer_kwargs):
        self.model_info = model_info
        self.device = infer_kwargs.get("device", "cpu")
        self.model = load_model(model_info, device_map=self.device)

    def _preprocess(self, inputs):
        """
        Preprocess the input before inference, including shape validation and value transformation.
        """
        # TODO: Integrate with the data processing pipeline operators
        pass

    def _postprocess(self, output: torch.Tensor):
        """
        Post-process the outputs after the entire inference task.
        """
        pass


class ForecastPipeline(BasicPipeline):
    def __init__(self, model_info, **infer_kwargs):
        super().__init__(model_info, infer_kwargs=infer_kwargs)

    def _preprocess(self, inputs):
        if len(inputs.shape) != 2:
            raise InferenceModelInternalError(
                f"[Inference] Input shape must be: [batch_size, seq_len], but receives {inputs.shape}"
            )
        return inputs

    def forecast(self, inputs, **infer_kwargs):
        pass

    def _postprocess(self, output: torch.Tensor):
        pass


class ClassificationPipeline(BasicPipeline):
    def __init__(self, model_info, **infer_kwargs):
        super().__init__(model_info, infer_kwargs=infer_kwargs)

    def _preprocess(self, inputs):
        pass

    def classify(self, inputs, **kwargs):
        pass

    def _post_decode(self):
        pass

    def _postprocess(self, output: torch.Tensor):
        pass


class ChatPipeline(BasicPipeline):
    def __init__(self, model_info, **infer_kwargs):
        super().__init__(model_info, infer_kwargs=infer_kwargs)

    def _preprocess(self, inputs):
        pass

    def chat(self, inputs, **kwargs):
        pass

    def _post_decode(self):
        pass

    def _postprocess(self, output: torch.Tensor):
        pass
