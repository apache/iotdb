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

import json
import os

import safetensors.torch as safetorch
from transformers import PreTrainedModel

from iotdb.ainode.core.log import Logger

from .configuration_toto import TotoConfig
from .model.attention import XFORMERS_AVAILABLE
from .model.backbone import TotoBackbone
from .model.toto import Toto
from .model.transformer import XFORMERS_SWIGLU_AVAILABLE

logger = Logger()


class TotoPreTrainedModel(PreTrainedModel):
    """Abstract base class for all Toto model variants."""

    config_class = TotoConfig
    base_model_prefix = "model"
    supports_gradient_checkpointing = False

    def _init_weights(self, module):
        # Weights are loaded from the pretrained checkpoint; no random initialisation needed.
        pass


class TotoForPrediction(TotoPreTrainedModel):
    """
    Toto (Timeseries-Optimized Transformer for Observability) model for time series prediction.

    Integrates the Toto backbone with AINode's model loading mechanism using the
    transformers PreTrainedModel interface. Weights are loaded directly from the
    Datadog/Toto-Open-Base-1.0 safetensors checkpoint.

    The backbone is stored as ``self.model`` so that safetensors key prefixes
    (``model.*``) map directly to parameters without any renaming.

    Reference: https://huggingface.co/Datadog/Toto-Open-Base-1.0
    """

    def __init__(self, config: TotoConfig):
        super().__init__(config)
        # Backbone stored as self.model so safetensors keys (model.*) match directly.
        self.model = TotoBackbone(
            patch_size=config.patch_size,
            stride=config.stride,
            embed_dim=config.embed_dim,
            num_layers=config.num_layers,
            num_heads=config.num_heads,
            mlp_hidden_dim=config.mlp_hidden_dim,
            dropout=config.dropout,
            spacewise_every_n_layers=config.spacewise_every_n_layers,
            scaler_cls=config.scaler_cls,
            output_distribution_classes=config.output_distribution_classes,
            output_distribution_kwargs=config.output_distribution_kwargs,
            spacewise_first=config.spacewise_first,
            use_memory_efficient_attention=config.use_memory_efficient_attention,
            stabilize_with_global=config.stabilize_with_global,
            scale_factor_exponent=config.scale_factor_exponent,
        )
        self.post_init()

    @classmethod
    def from_pretrained(cls, pretrained_model_name_or_path, **kwargs):
        """
        Load TotoForPrediction from a local directory containing ``config.json``
        and ``model.safetensors``.

        This override is required because:
        1. The safetensors file uses legacy SwiGLU key names that need remapping.
        2. The config uses class-path strings for ``scaler_cls`` and
           ``output_distribution_classes`` that must not be filtered out.

        Args:
            pretrained_model_name_or_path (str): Path to a local directory.
            **kwargs: Extra key/value pairs merged into the config before construction.

        Returns:
            TotoForPrediction: Fully initialised and weight-loaded model in eval mode.
        """
        if os.path.isdir(pretrained_model_name_or_path):
            config_file = os.path.join(pretrained_model_name_or_path, "config.json")
            safetensors_file = os.path.join(
                pretrained_model_name_or_path, "model.safetensors"
            )
        else:
            raise ValueError(
                f"pretrained_model_name_or_path must be a local directory, "
                f"got: {pretrained_model_name_or_path}"
            )

        # ── Load config ──────────────────────────────────────────────────────
        config_dict: dict = {}
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                config_dict = json.load(f)
        config_dict.update(kwargs)

        # Disable xFormers memory-efficient attention if the library is absent.
        if not XFORMERS_AVAILABLE and config_dict.get(
            "use_memory_efficient_attention", True
        ):
            config_dict["use_memory_efficient_attention"] = False

        config = TotoConfig(**config_dict)

        # ── Instantiate model ─────────────────────────────────────────────────
        instance = cls(config)

        # ── Load safetensors weights ──────────────────────────────────────────
        if not os.path.exists(safetensors_file):
            raise FileNotFoundError(
                f"Model checkpoint not found at: {safetensors_file}"
            )

        state_dict = safetorch.load_file(safetensors_file, device="cpu")

        # Remap SwiGLU weight names if the fused xFormers kernel is available.
        use_fused_swiglu = XFORMERS_SWIGLU_AVAILABLE and not config_dict.get(
            "pre_xformers_checkpoint", False
        )
        state_dict = Toto._map_state_dict_keys(state_dict, use_fused_swiglu)

        # Filter to keys that exist in the model, skipping cached rotary buffers.
        model_state = instance.state_dict()
        filtered_state_dict = {
            k: v
            for k, v in state_dict.items()
            if k in model_state and not k.endswith("rotary_emb.freqs")
        }

        instance.load_state_dict(filtered_state_dict, strict=False)
        instance.eval()

        logger.info(f"Loaded Toto model from {pretrained_model_name_or_path}")
        return instance

    @property
    def backbone(self):
        """The underlying ``TotoBackbone`` used for inference."""
        return self.model

    @property
    def device(self):
        """Device on which model parameters reside."""
        return next(self.parameters()).device
