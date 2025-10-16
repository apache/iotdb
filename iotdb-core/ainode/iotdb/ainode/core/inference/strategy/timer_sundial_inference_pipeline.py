1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import torch
1
1from iotdb.ainode.core.exception import InferenceModelInternalError
1from iotdb.ainode.core.inference.strategy.abstract_inference_pipeline import (
1    AbstractInferencePipeline,
1)
1from iotdb.ainode.core.model.sundial.configuration_sundial import SundialConfig
1
1
1class TimerSundialInferencePipeline(AbstractInferencePipeline):
1    """
1    Strategy for Timer-Sundial model inference.
1    """
1
1    def __init__(self, model_config: SundialConfig, **infer_kwargs):
1        super().__init__(model_config, infer_kwargs=infer_kwargs)
1
1    def preprocess_inputs(self, inputs: torch.Tensor):
1        super().preprocess_inputs(inputs)
1        if len(inputs.shape) != 2:
1            raise InferenceModelInternalError(
1                f"[Inference] Input shape must be: [batch_size, seq_len], but receives {inputs.shape}"
1            )
1        # TODO: Disassemble and adapt with Sundial's ts_generation_mixin.py
1        return inputs
1
1    def post_decode(self):
1        # TODO: Disassemble and adapt with Sundial's ts_generation_mixin.py
1        pass
1
1    def post_inference(self):
1        # TODO: Disassemble and adapt with Sundial's ts_generation_mixin.py
1        pass
1