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
1from typing import List
1
1from transformers import PretrainedConfig
1
1
1class TimerConfig(PretrainedConfig):
1    model_type = "timer"
1    keys_to_ignore_at_inference = ["past_key_values"]
1
1    def __init__(
1        self,
1        input_token_len: int = 1,
1        hidden_size: int = 1024,
1        intermediate_size: int = 2048,
1        output_token_lens: List[int] = [1, 8, 32, 64],
1        num_hidden_layers: int = 8,
1        num_attention_heads: int = 8,
1        hidden_act: str = "silu",
1        use_cache: bool = True,
1        rope_theta: int = 10000,
1        attention_dropout: float = 0.0,
1        initializer_range: float = 0.02,
1        max_position_embeddings: int = 10000,
1        **kwargs,
1    ):
1        self.input_token_len = input_token_len
1        self.hidden_size = hidden_size
1        self.intermediate_size = intermediate_size
1        self.num_hidden_layers = num_hidden_layers
1        self.num_attention_heads = num_attention_heads
1        self.hidden_act = hidden_act
1        self.output_token_lens = output_token_lens
1        self.use_cache = use_cache
1        self.rope_theta = rope_theta
1        self.attention_dropout = attention_dropout
1        self.initializer_range = initializer_range
1        self.max_position_embeddings = max_position_embeddings
1
1        super().__init__(
1            **kwargs,
1        )
1