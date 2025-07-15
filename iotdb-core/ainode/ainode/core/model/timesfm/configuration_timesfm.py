# configuration_timesfm.py
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

from transformers import PretrainedConfig
from typing import List

class TimesFmConfig(PretrainedConfig):
    model_type = "timesfm"
    
    def __init__(
        self,
        patch_length: int = 32,
        context_length: int = 2048,
        horizon_length: int = 128,
        hidden_size: int = 1280,
        intermediate_size: int = 1280,
        num_hidden_layers: int = 50,
        num_attention_heads: int = 16,
        head_dim: int = 80,
        freq_size: int = 3,
        use_positional_embedding: bool = False,
        min_timescale: int = 1,
        max_timescale: int = 10000,
        attention_dropout: float = 0.0,
        initializer_range: float = 0.02,
        rms_norm_eps: float = 1e-6,
        pad_val: float = 1123581321.0,
        tolerance: float = 1e-6,
        quantiles: List[float] = None,
        **kwargs,
    ):
        if quantiles is None:
            quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        
        super().__init__(**kwargs)
        
        self.patch_length = patch_length
        self.context_length = context_length
        self.horizon_length = horizon_length
        self.hidden_size = hidden_size
        self.intermediate_size = intermediate_size
        self.num_hidden_layers = num_hidden_layers
        self.num_attention_heads = num_attention_heads
        self.head_dim = head_dim
        self.freq_size = freq_size
        self.use_positional_embedding = use_positional_embedding
        self.min_timescale = min_timescale
        self.max_timescale = max_timescale
        self.attention_dropout = attention_dropout
        self.initializer_range = initializer_range
        self.rms_norm_eps = rms_norm_eps
        self.pad_val = pad_val
        self.tolerance = tolerance
        self.quantiles = quantiles

try:
    from transformers import AutoConfig
    AutoConfig.register("timesfm", TimesFmConfig)
except Exception:
    pass