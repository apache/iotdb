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
# This file includes code derived from DataDog/toto
# (https://github.com/DataDog/toto), licensed under the Apache-2.0 License.
# Copyright 2025 Datadog, Inc.

import json
import os
import re
from pathlib import Path
from typing import Dict, Optional, Union

import safetensors.torch as safetorch
import torch

from .attention import XFORMERS_AVAILABLE
from .backbone import TotoBackbone
from .transformer import XFORMERS_SWIGLU_AVAILABLE


class Toto(torch.nn.Module):
    """
    PyTorch module for Toto (Timeseries-Optimized Transformer for Observability).
    This class is used internally for checkpoint loading logic.
    """

    def __init__(
        self,
        patch_size: int,
        stride: int,
        embed_dim: int,
        num_layers: int,
        num_heads: int,
        mlp_hidden_dim: int,
        dropout: float,
        spacewise_every_n_layers: int,
        scaler_cls: str,
        output_distribution_classes: list[str],
        spacewise_first: bool = True,
        output_distribution_kwargs: dict | None = None,
        use_memory_efficient_attention: bool = True,
        stabilize_with_global: bool = True,
        scale_factor_exponent: float = 10.0,
        **model_kwargs,
    ):
        super().__init__()
        self.model = TotoBackbone(
            patch_size=patch_size,
            stride=stride,
            embed_dim=embed_dim,
            num_layers=num_layers,
            num_heads=num_heads,
            mlp_hidden_dim=mlp_hidden_dim,
            dropout=dropout,
            spacewise_every_n_layers=spacewise_every_n_layers,
            scaler_cls=scaler_cls,
            output_distribution_classes=output_distribution_classes,
            spacewise_first=spacewise_first,
            output_distribution_kwargs=output_distribution_kwargs,
            use_memory_efficient_attention=use_memory_efficient_attention,
            stabilize_with_global=stabilize_with_global,
            scale_factor_exponent=scale_factor_exponent,
        )

    @classmethod
    def load_from_checkpoint(
        cls,
        checkpoint_path,
        map_location: str = "cpu",
        strict=True,
        **model_kwargs,
    ):
        if os.path.isdir(checkpoint_path):
            safetensors_file = os.path.join(checkpoint_path, "model.safetensors")
        else:
            safetensors_file = checkpoint_path

        if os.path.exists(safetensors_file):
            model_state = safetorch.load_file(safetensors_file, device=map_location)
        else:
            raise FileNotFoundError(
                f"Model checkpoint not found at: {safetensors_file}"
            )

        config_file = os.path.join(checkpoint_path, "config.json")
        config = {}
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                config = json.load(f)

        config.update(model_kwargs)

        remapped_state_dict = cls._map_state_dict_keys(
            model_state,
            XFORMERS_SWIGLU_AVAILABLE
            and not config.get("pre_xformers_checkpoint", False),
        )

        if not XFORMERS_AVAILABLE and config.get(
            "use_memory_efficient_attention", True
        ):
            config["use_memory_efficient_attention"] = False

        instance = cls(**config)
        instance.to(map_location)

        filtered_remapped_state_dict = {
            k: v
            for k, v in remapped_state_dict.items()
            if k in instance.state_dict() and not k.endswith("rotary_emb.freqs")
        }

        instance.load_state_dict(filtered_remapped_state_dict, strict=strict)
        return instance

    @staticmethod
    def _map_state_dict_keys(state_dict, use_fused_swiglu):
        if use_fused_swiglu:
            remap_keys = {
                "mlp.0.weight": "mlp.0.w12.weight",
                "mlp.0.bias": "mlp.0.w12.bias",
                "mlp.2.weight": "mlp.0.w3.weight",
                "mlp.2.bias": "mlp.0.w3.bias",
            }
        else:
            remap_keys = {
                "mlp.0.w12.weight": "mlp.0.weight",
                "mlp.0.w12.bias": "mlp.0.bias",
                "mlp.0.w3.weight": "mlp.2.weight",
                "mlp.0.w3.bias": "mlp.2.bias",
            }

        def replace_key(text):
            for pattern, replacement in remap_keys.items():
                text = re.sub(pattern, replacement, text)
            return text

        return {replace_key(k): v for k, v in state_dict.items()}

    @property
    def device(self):
        return next(self.model.parameters()).device
