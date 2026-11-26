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

from transformers import PretrainedConfig


class SundialConfig(PretrainedConfig):
    model_type = "sundial"
    keys_to_ignore_at_inference = ["past_key_values"]

    def __init__(
        self,
        input_token_len: int = 16,
        hidden_size: int = 768,
        intermediate_size: int = 3072,
        output_token_lens: List[int] = [720],
        num_hidden_layers: int = 12,
        num_attention_heads: int = 12,
        hidden_act: str = "silu",
        use_cache: bool = True,
        rope_theta: int = 10000,
        dropout_rate: float = 0.1,
        initializer_range: float = 0.02,
        max_position_embeddings: int = 10000,
        flow_loss_depth: int = 3,
        num_sampling_steps: int = 50,
        diffusion_batch_mul: int = 4,
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
        self.dropout_rate = dropout_rate
        self.initializer_range = initializer_range
        self.max_position_embeddings = max_position_embeddings
        self.flow_loss_depth = flow_loss_depth
        self.num_sampling_steps = num_sampling_steps
        self.diffusion_batch_mul = diffusion_batch_mul

        super().__init__(
            **kwargs,
        )
