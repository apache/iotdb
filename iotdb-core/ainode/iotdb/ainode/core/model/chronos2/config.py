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

from dataclasses import dataclass
from typing import List, Literal

from transformers.configuration_utils import PretrainedConfig


class Chronos2CoreConfig(PretrainedConfig):
    """
    HF transformers-style pretrained model config for Chronos-2.0, based on T5Config.

    Arguments
    ----------
    d_model
        Size model's hidden states, by default 512
    d_kv
        Size of the key, query, value projections per attention head, by default 64
    d_ff
        Size of the intermediate feed forward layers, by default 2048
    num_layers
        Number of hidden layers in the encoder, by default 6
    num_heads
        Number of attention heads for each attention layer, by default 8
    dropout_rate
        The ratio for all dropout layers, by default 0.1
    layer_norm_epsilon
        The epsilon used by the layer normalization layers, by default 1e-6
    initializer_factor
        A factor for initializing all weight matrices, by default 0.05
    feed_forward_proj
        Type of feed forward layer to be used, by default "relu"
    vocab_size
        Size of vocabulary for special tokens, by default 2
    pad_token_id
        Token ID for padding/missing value token, by default 0
    rope_theta
        The base theta for rotary position embedding (RoPE), by default 10000.0
    attn_implementation
        The attention implementation to use. Options: "eager" or "sdpa", by default None (uses "sdpa")
    """

    model_type = "t5"
    attribute_map = {
        "hidden_size": "d_model",
        "num_attention_heads": "num_heads",
        "num_hidden_layers": "num_layers",
        "head_dim": "d_kv",
    }

    def __init__(
        self,
        d_model: int = 512,
        d_kv: int = 64,
        d_ff: int = 2048,
        num_layers: int = 6,
        num_heads: int = 8,
        dropout_rate: float = 0.1,
        layer_norm_epsilon: float = 1e-6,
        initializer_factor: float = 0.05,
        feed_forward_proj: str = "relu",
        vocab_size: int = 2,
        pad_token_id: int = 0,
        rope_theta: float = 10000.0,
        attn_implementation: Literal["eager", "sdpa"] | None = None,
        **kwargs,
    ):
        self.vocab_size = vocab_size
        self.d_model = d_model
        self.d_kv = d_kv
        self.d_ff = d_ff
        self.num_layers = num_layers
        self.num_heads = num_heads
        self.dropout_rate = dropout_rate
        self.layer_norm_epsilon = layer_norm_epsilon
        self.initializer_factor = initializer_factor
        self.feed_forward_proj = feed_forward_proj
        self.rope_theta = rope_theta

        act_info = self.feed_forward_proj.split("-")
        self.dense_act_fn = act_info[-1]
        self.is_gated_act = act_info[0] == "gated"

        assert not self.is_gated_act, "gated activation is not supported"

        # Attention implementation - default to "sdpa" if not specified
        attn_implementation = attn_implementation or "sdpa"
        assert attn_implementation in [
            "eager",
            "sdpa",
        ], f"attn_implementation {attn_implementation} not supported"

        # unused
        kwargs.pop("is_encoder_decoder", None)
        kwargs.pop("eos_token_id", None)

        super().__init__(
            pad_token_id=pad_token_id,
            is_encoder_decoder=False,
            attn_implementation=attn_implementation,
            **kwargs,
        )


@dataclass
class Chronos2ForecastingConfig:
    context_length: int
    output_patch_size: int
    input_patch_size: int
    input_patch_stride: int
    quantiles: List[float]
    use_reg_token: bool = False
    use_arcsinh: bool = False
    max_output_patches: int = 1
    time_encoding_scale: int | None = None

    @classmethod
    def editable_fields(cls) -> list[str]:
        """
        Fields that maybe modified during the fine-tuning stage.
        """
        return ["context_length", "max_output_patches"]
