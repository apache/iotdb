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

from typing import List, Tuple

from transformers import PretrainedConfig


class Moirai2Config(PretrainedConfig):
    model_type = "moirai2"

    def __init__(
        self,
        d_model: int = 384,
        d_ff: int = 1024,
        num_layers: int = 6,
        patch_size: int = 16,
        max_seq_len: int = 512,
        attn_dropout_p: float = 0.0,
        dropout_p: float = 0.0,
        scaling: bool = True,
        num_predict_token: int = 4,
        quantile_levels: Tuple[float, ...] = (
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
        ),
        **kwargs,
    ):
        self.d_model = d_model
        self.d_ff = d_ff
        self.num_layers = num_layers
        self.patch_size = patch_size
        self.max_seq_len = max_seq_len
        self.attn_dropout_p = attn_dropout_p
        self.dropout_p = dropout_p
        self.scaling = scaling
        self.num_predict_token = num_predict_token
        self.quantile_levels = quantile_levels
        super().__init__(**kwargs)
