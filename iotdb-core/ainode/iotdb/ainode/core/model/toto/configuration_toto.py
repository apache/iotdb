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

from typing import List, Optional

from transformers import PretrainedConfig


class TotoConfig(PretrainedConfig):
    """
    Configuration class for the Toto time series forecasting model.

    Toto (Time Series Optimized Transformer for Observability) is a foundation model
    for multivariate time series forecasting developed by Datadog. It uses a decoder-only
    architecture with per-variate patch-based causal scaling, proportional time-variate
    factorized attention, and a Student-T mixture prediction head.

    Reference: https://github.com/DataDog/toto
    """

    model_type = "toto"

    def __init__(
        self,
        patch_size: int = 32,
        stride: int = 32,
        embed_dim: int = 1024,
        num_layers: int = 18,
        num_heads: int = 16,
        mlp_hidden_dim: int = 2816,
        dropout: float = 0.0,
        spacewise_every_n_layers: int = 3,
        scaler_cls: str = "per_variate_causal",
        output_distribution_classes: Optional[List[str]] = None,
        output_distribution_kwargs: Optional[dict] = None,
        spacewise_first: bool = True,
        use_memory_efficient_attention: bool = True,
        stabilize_with_global: bool = True,
        scale_factor_exponent: float = 10.0,
        **kwargs,
    ):
        self.patch_size = patch_size
        self.stride = stride
        self.embed_dim = embed_dim
        self.num_layers = num_layers
        self.num_heads = num_heads
        self.mlp_hidden_dim = mlp_hidden_dim
        self.dropout = dropout
        self.spacewise_every_n_layers = spacewise_every_n_layers
        self.scaler_cls = scaler_cls
        self.output_distribution_classes = output_distribution_classes or [
            "student_t_mixture"
        ]
        # k_components=5 is the default used by Datadog/Toto-Open-Base-1.0
        self.output_distribution_kwargs = output_distribution_kwargs or {
            "k_components": 5
        }
        self.spacewise_first = spacewise_first
        self.use_memory_efficient_attention = use_memory_efficient_attention
        self.stabilize_with_global = stabilize_with_global
        self.scale_factor_exponent = scale_factor_exponent

        super().__init__(**kwargs)
