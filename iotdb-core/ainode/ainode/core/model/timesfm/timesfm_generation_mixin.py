# timesfm_generation_mixin.py 
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

import torch
from typing import List, Optional, Union, Sequence
from transformers import GenerationMixin

class TimesFmGenerationMixin(GenerationMixin):
    
    @torch.no_grad()
    def generate(
        self,
        inputs: Union[torch.Tensor, Sequence[torch.Tensor]] = None,
        freq: Optional[Union[int, List[int], Sequence[int]]] = None,
        horizon_length: Optional[int] = None,
        window_size: Optional[int] = None,
        truncate_negative: bool = False,
        **kwargs,
    ) -> torch.Tensor:
        if inputs is None:
            raise ValueError("inputs cannot be None")
            
        if isinstance(inputs, torch.Tensor):
            if len(inputs.shape) == 1:
                inputs = [inputs]
            elif len(inputs.shape) == 2:
                inputs = [inputs[i] for i in range(inputs.shape[0])]
            else:
                raise ValueError("Input tensor must be 1D or 2D")
        
        if freq is None:
            freq = [0] * len(inputs) 
        elif isinstance(freq, int):
            freq = [freq] * len(inputs)
        elif len(freq) != len(inputs):
            raise ValueError("freq length must match inputs length")
        
        if horizon_length is None:
            horizon_length = getattr(self.config, 'horizon_length', 128)
            
        result = self.forward(
            past_values=inputs,
            freq=freq,
            window_size=window_size,
            truncate_negative=truncate_negative,
            **kwargs
        )
        
        if hasattr(result, 'mean_predictions'):
            return result.mean_predictions
        elif hasattr(result, 'predictions'):
            return result.predictions
        else:
            return result
    
    def prepare_inputs_for_generation(self, inputs, **kwargs):
        if isinstance(inputs, torch.Tensor):
            if len(inputs.shape) == 2:
                past_values = [inputs[i] for i in range(inputs.shape[0])]
            else:
                past_values = [inputs]
        else:
            past_values = inputs
            
        return {
            "past_values": past_values,
            "freq": kwargs.get("freq", [0] * len(past_values)),
            **kwargs
        }