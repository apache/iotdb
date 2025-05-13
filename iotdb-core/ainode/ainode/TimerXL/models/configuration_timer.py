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
from typing import List

class TimerxlConfig:
    model_type = "timerxl"

    def __init__(
        self,
        input_token_len: int = 96,          # how many points as a token, don't change
        hidden_size: int = 1024,            # model hidden size
        intermediate_size: int = 2048,      # ffn middle size
        output_token_lens: List[int] = [96],# how many points as a token, don't change
        num_hidden_layers: int = 8,
        num_attention_heads: int = 8,
        hidden_act: str = "silu",           # activation function
        use_cache: bool = True,             # kv cache
        rope_theta: int = 10000,            # ROBE parameter
        attention_dropout: float = 0.0,     
        initializer_range: float = 0.02,    # be of no use, because we already have weights
        max_position_embeddings: int = 10000,
        ckpt_path: str = None,              # weight path
        **kwargs,
    ):
        self.input_token_len = input_token_len
        self.hidden_size = hidden_size
        self.intermediate_size = intermediate_size
        self.num_hidden_layers = num_hidden_layers
        self.num_attention_heads = num_attention_heads
        self.hidden_act = hidden_act
        self.output_token_lens = output_token_lens
        self.use_cache = use_cache
        self.rope_theta = rope_theta
        self.attention_dropout = attention_dropout
        self.initializer_range = initializer_range
        self.max_position_embeddings = max_position_embeddings
        self.ckpt_path = ckpt_path

        super().__init__(
            **kwargs,
        )
        
    @classmethod
    def from_dict(cls, config_dict: dict) -> "TimerxlConfig":
        return cls(**config_dict)
