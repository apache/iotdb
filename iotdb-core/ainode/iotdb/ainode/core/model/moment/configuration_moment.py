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

# This file contains code adapted from the MOMENT project
# (https://github.com/moment-timeseries-foundation-model/moment),
# originally licensed under the MIT License.

from typing import Optional

from transformers import PretrainedConfig


class MomentConfig(PretrainedConfig):
    """
    Configuration class for the MOMENT time series foundation model.

    MOMENT (A Family of Open Time-series Foundation Models) is developed by
    Auton Lab, Carnegie Mellon University. It uses a T5 encoder-only backbone
    with patch-based input embedding and RevIN normalization for multi-task
    time series analysis including forecasting, classification, anomaly
    detection and imputation.

    Reference: https://arxiv.org/abs/2402.03885
    """

    model_type = "moment"

    def __init__(
        self,
        seq_len: int = 512,
        patch_len: int = 8,
        patch_stride_len: int = 8,
        d_model: Optional[int] = None,
        transformer_backbone: str = "google/flan-t5-large",
        forecast_horizon: int = 96,
        revin_affine: bool = False,
        t5_config: Optional[dict] = None,
        **kwargs,
    ):
        self.seq_len = seq_len
        self.patch_len = patch_len
        self.patch_stride_len = patch_stride_len
        self.transformer_backbone = transformer_backbone
        self.forecast_horizon = forecast_horizon
        self.revin_affine = revin_affine
        self.t5_config = t5_config

        # Infer d_model: prefer explicit value, then t5_config, then default
        if d_model is not None:
            self.d_model = d_model
        elif t5_config is not None and "d_model" in t5_config:
            self.d_model = t5_config["d_model"]
        else:
            self.d_model = 1024  # Default for MOMENT-1-large

        super().__init__(**kwargs)
