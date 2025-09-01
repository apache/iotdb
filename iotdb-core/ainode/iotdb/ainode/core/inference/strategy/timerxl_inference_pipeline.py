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

from iotdb.ainode.core.exception import InferenceModelInternalError
from iotdb.ainode.core.inference.strategy.abstract_inference_pipeline import (
    AbstractInferencePipeline,
)
from iotdb.ainode.core.model.timerxl.configuration_timer import TimerConfig


class TimerXLInferencePipeline(AbstractInferencePipeline):
    """
    Strategy for Timer-XL model inference.
    """

    def __init__(self, model_config: TimerConfig, **infer_kwargs):
        super().__init__(model_config, infer_kwargs=infer_kwargs)

    def preprocess_inputs(self, inputs: torch.Tensor):
        super().preprocess_inputs(inputs)
        if len(inputs.shape) != 2:
            raise InferenceModelInternalError(
                f"[Inference] Input shape must be: [batch_size, seq_len], but receives {inputs.shape}"
            )
        # TODO: Disassemble and adapt with TimerXL's ts_generation_mixin.py
        return inputs

    def post_decode(self):
        # TODO: Disassemble and adapt with TimerXL's ts_generation_mixin.py
        pass

    def post_inference(self):
        # TODO: Disassemble and adapt with TimerXL's ts_generation_mixin.py
        pass
