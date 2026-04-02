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

import json
import os
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
from transformers import PreTrainedModel, T5Config, T5EncoderModel

from iotdb.ainode.core.log import Logger

from .configuration_moment import MomentConfig

logger = Logger()


@dataclass
class MomentOutput:
    forecast: Optional[torch.Tensor] = None
    reconstruction: Optional[torch.Tensor] = None
    embeddings: Optional[torch.Tensor] = None
    input_mask: Optional[torch.Tensor] = None


class RevIN(nn.Module):
    """Reversible Instance Normalization for time series."""

    def __init__(self, n_features: int, affine: bool = False, eps: float = 1e-5):
        super().__init__()
        self.n_features = n_features
        self.affine = affine
        self.eps = eps
        if self.affine:
            self.affine_weight = nn.Parameter(torch.ones(self.n_features))
            self.affine_bias = nn.Parameter(torch.zeros(self.n_features))

    def forward(
        self, x: torch.Tensor, mode: str, mask: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        # x: [batch, n_channels, seq_len]
        # mask: [batch, seq_len] - 1 for observed, 0 for padding (optional)
        if mode == "norm":
            self._get_statistics(x, mask)
            x = self._normalize(x)
        elif mode == "denorm":
            x = self._denormalize(x)
        return x

    def _get_statistics(self, x: torch.Tensor, mask: Optional[torch.Tensor] = None):
        if mask is not None:
            # Expand mask to match x: [batch, 1, seq_len] -> broadcast over n_channels
            m = mask.unsqueeze(1).float()  # [batch, 1, seq_len]
            count = m.sum(dim=-1, keepdim=True).clamp(min=1)
            self.mean = (x * m).sum(dim=-1, keepdim=True) / count
            self.stdev = torch.sqrt(
                ((x - self.mean) * m).pow(2).sum(dim=-1, keepdim=True) / count
                + self.eps
            )
            self.mean = self.mean.detach()
            self.stdev = self.stdev.detach()
        else:
            self.mean = torch.mean(x, dim=-1, keepdim=True).detach()
            self.stdev = torch.sqrt(
                torch.var(x, dim=-1, keepdim=True, unbiased=False) + self.eps
            ).detach()

    def _normalize(self, x: torch.Tensor) -> torch.Tensor:
        x = (x - self.mean) / self.stdev
        if self.affine:
            x = x * self.affine_weight.unsqueeze(0).unsqueeze(-1)
            x = x + self.affine_bias.unsqueeze(0).unsqueeze(-1)
        return x

    def _denormalize(self, x: torch.Tensor) -> torch.Tensor:
        if self.affine:
            x = x - self.affine_bias.unsqueeze(0).unsqueeze(-1)
            x = x / (self.affine_weight.unsqueeze(0).unsqueeze(-1) + self.eps)
        x = x * self.stdev
        x = x + self.mean
        return x


class Patching(nn.Module):
    """Unfold a 1-D time series into fixed-size patches."""

    def __init__(self, patch_len: int = 8, stride: int = 8):
        super().__init__()
        self.patch_len = patch_len
        self.stride = stride

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [batch, n_channels, seq_len]
        # out: [batch, n_channels, n_patches, patch_len]
        x = x.unfold(dimension=-1, size=self.patch_len, step=self.stride)
        return x


class PatchEmbedding(nn.Module):
    """Linear projection of patches to model dimension."""

    def __init__(self, d_model: int, patch_len: int):
        super().__init__()
        self.proj = nn.Linear(patch_len, d_model)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [batch, n_channels, n_patches, patch_len]
        # out: [batch, n_channels, n_patches, d_model]
        return self.proj(x)


class ForecastingHead(nn.Module):
    """Linear head that projects flattened patch embeddings to forecast horizon."""

    def __init__(self, d_model: int, n_patches: int, forecast_horizon: int):
        super().__init__()
        self.flatten = nn.Flatten(start_dim=-2)
        self.proj = nn.Linear(d_model * n_patches, forecast_horizon)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: [batch, n_channels, n_patches, d_model]
        x = self.flatten(x)  # [batch, n_channels, n_patches * d_model]
        return self.proj(x)  # [batch, n_channels, forecast_horizon]


class MomentBackbone(nn.Module):
    """
    Core MOMENT architecture.

    Architecture:
        Input [batch, n_channels, seq_len]
        -> RevIN normalization
        -> Patching (unfold into fixed-size patches)
        -> Patch embedding (linear projection to d_model)
        -> T5 Encoder (self-attention layers)
        -> Task-specific head (forecasting: linear projection)
        -> RevIN denormalization
        -> Output [batch, n_channels, forecast_horizon]
    """

    def __init__(self, config: MomentConfig):
        super().__init__()
        self.config = config
        self.seq_len = config.seq_len
        self.patch_len = config.patch_len
        self.patch_stride_len = config.patch_stride_len
        self.d_model = config.d_model
        self.forecast_horizon = config.forecast_horizon

        self.n_patches = (self.seq_len - self.patch_len) // self.patch_stride_len + 1

        # RevIN normalization
        self.revin = RevIN(n_features=1, affine=config.revin_affine)

        # Patching and embedding
        self.patching = Patching(patch_len=self.patch_len, stride=self.patch_stride_len)
        self.patch_embedding = PatchEmbedding(
            d_model=self.d_model, patch_len=self.patch_len
        )

        # Positional embedding for patches
        self.position_embedding = nn.Embedding(self.n_patches, self.d_model)

        # Mask embedding (for masked reconstruction during pre-training)
        self.mask_embedding = nn.Parameter(torch.zeros(self.d_model))

        # T5 encoder backbone
        t5_config = getattr(config, "t5_config", None)
        if t5_config is not None:
            encoder_config = T5Config(**t5_config)
        else:
            encoder_config = T5Config.from_pretrained(config.transformer_backbone)
            encoder_config.d_model = self.d_model
        self.encoder = T5EncoderModel(encoder_config)

        # Layer norm before head
        self.layer_norm = nn.LayerNorm(self.d_model)

        # Forecasting head
        self.head = ForecastingHead(
            d_model=self.d_model,
            n_patches=self.n_patches,
            forecast_horizon=self.forecast_horizon,
        )

    def forward(
        self,
        x_enc: torch.Tensor,
        input_mask: Optional[torch.Tensor] = None,
    ) -> MomentOutput:
        """
        Forward pass for forecasting.

        Args:
            x_enc: [batch_size, n_channels, seq_len]
            input_mask: [batch_size, seq_len] - 1 for observed, 0 for padding

        Returns:
            MomentOutput with forecast field
        """
        batch_size, n_channels, seq_len = x_enc.shape

        # Handle input_mask
        if input_mask is None:
            input_mask = torch.ones(batch_size, seq_len, device=x_enc.device)

        # RevIN normalization (channel-independent)
        # Reshape to process each channel independently
        x = x_enc.reshape(batch_size * n_channels, 1, seq_len)
        # Expand input_mask to match reshaped x: repeat for each channel
        revin_mask = input_mask.unsqueeze(1).expand(-1, n_channels, -1)
        revin_mask = revin_mask.reshape(batch_size * n_channels, seq_len)
        x = self.revin(x, mode="norm", mask=revin_mask)
        x = x.reshape(batch_size, n_channels, seq_len)

        # Patching: [batch, n_channels, n_patches, patch_len]
        x = self.patching(x)

        # Patch embedding: [batch, n_channels, n_patches, d_model]
        x = self.patch_embedding(x)

        # Apply input mask at patch level
        patch_mask = self._create_patch_mask(input_mask)
        mask_embed = self.mask_embedding.unsqueeze(0).unsqueeze(0).unsqueeze(0)
        x = x * patch_mask.unsqueeze(-1) + mask_embed * (1.0 - patch_mask.unsqueeze(-1))

        # Position embedding
        positions = torch.arange(self.n_patches, device=x.device)
        x = x + self.position_embedding(positions).unsqueeze(0).unsqueeze(0)

        # Flatten batch and channel dims for T5 encoder
        # [batch * n_channels, n_patches, d_model]
        x = x.reshape(batch_size * n_channels, self.n_patches, self.d_model)

        # T5 encoder forward
        enc_output = self.encoder(inputs_embeds=x).last_hidden_state

        # Layer norm
        enc_output = self.layer_norm(enc_output)

        # Restore channel dim: [batch, n_channels, n_patches, d_model]
        enc_output = enc_output.reshape(
            batch_size, n_channels, self.n_patches, self.d_model
        )

        # Forecasting head: [batch, n_channels, forecast_horizon]
        forecast = self.head(enc_output)

        # RevIN denormalization
        forecast = forecast.reshape(batch_size * n_channels, 1, self.forecast_horizon)
        forecast = self.revin(forecast, mode="denorm")
        forecast = forecast.reshape(batch_size, n_channels, self.forecast_horizon)

        return MomentOutput(forecast=forecast, embeddings=enc_output)

    def _create_patch_mask(self, input_mask: torch.Tensor) -> torch.Tensor:
        """Convert per-timestep mask to per-patch mask via average pooling."""
        # input_mask: [batch, seq_len]
        # output: [batch, 1, n_patches] with values in [0, 1]
        mask = input_mask.unsqueeze(1)  # [batch, 1, seq_len]
        mask = mask.unfold(
            dimension=-1, size=self.patch_len, step=self.patch_stride_len
        )
        # [batch, 1, n_patches, patch_len]
        mask = mask.mean(dim=-1)  # [batch, 1, n_patches]
        mask = (mask > 0.5).float()
        return mask


class MomentPreTrainedModel(PreTrainedModel):
    """Abstract base class for all MOMENT model variants."""

    config_class = MomentConfig
    base_model_prefix = "moment"
    supports_gradient_checkpointing = False

    def _init_weights(self, module):
        pass


class MomentForPrediction(MomentPreTrainedModel):
    """
    MOMENT model for time series forecasting, wrapped as a HuggingFace PreTrainedModel.

    Loads the pre-trained MOMENT backbone from safetensors and configures
    the forecasting head for a given horizon.

    Reference: https://huggingface.co/AutonLab/MOMENT-1-large
    """

    def __init__(self, config: MomentConfig):
        super().__init__(config)
        self.moment = MomentBackbone(config)
        self.post_init()

    def forward(
        self,
        x_enc: torch.Tensor,
        input_mask: Optional[torch.Tensor] = None,
    ) -> MomentOutput:
        return self.moment(x_enc=x_enc, input_mask=input_mask)

    @classmethod
    def from_pretrained(cls, pretrained_model_name_or_path, **kwargs):
        """
        Load MomentForPrediction from a local directory containing
        ``config.json`` and ``model.safetensors``.

        The upstream MOMENT checkpoint uses a flat state-dict structure
        (keys like ``revin.affine_weight``, ``encoder.encoder.block.0...``).
        This method handles mapping those keys into our nested ``moment.*``
        structure.
        """
        # Pop kwargs injected by load_transformers_model that are not
        # relevant to our custom loading logic.
        kwargs.pop("config", None)
        kwargs.pop("trust_remote_code", None)

        if not os.path.isdir(pretrained_model_name_or_path):
            raise ValueError(
                f"pretrained_model_name_or_path must be a local directory, "
                f"got: {pretrained_model_name_or_path}"
            )

        config_file = os.path.join(pretrained_model_name_or_path, "config.json")
        safetensors_file = os.path.join(
            pretrained_model_name_or_path, "model.safetensors"
        )

        # Load config
        config_dict = {}
        if os.path.exists(config_file):
            with open(config_file, "r") as f:
                config_dict = json.load(f)

        # Extract t5_config if present in the upstream config
        t5_config = config_dict.pop("t5_config", None)

        forecast_horizon = kwargs.pop("forecast_horizon", 96)

        # Build MomentConfig with t5_config stored for backbone construction
        config = MomentConfig(
            seq_len=config_dict.get("seq_len", 512),
            patch_len=config_dict.get("patch_len", 8),
            patch_stride_len=config_dict.get("patch_stride_len", 8),
            d_model=config_dict.get("d_model"),
            transformer_backbone=config_dict.get(
                "transformer_backbone", "google/flan-t5-large"
            ),
            forecast_horizon=forecast_horizon,
            revin_affine=config_dict.get("revin_affine", False),
            t5_config=t5_config,
        )

        # Instantiate model
        instance = cls.__new__(cls)
        MomentPreTrainedModel.__init__(instance, config)
        instance.moment = MomentBackbone(config)
        instance.post_init()

        # Load weights
        if not os.path.exists(safetensors_file):
            raise FileNotFoundError(
                f"Model checkpoint not found at: {safetensors_file}"
            )

        import safetensors.torch as safetorch

        state_dict = safetorch.load_file(safetensors_file, device="cpu")

        # Map upstream flat keys to our nested moment.* structure
        mapped_state_dict = {
            f"moment.{key}": value for key, value in state_dict.items()
        }

        # Load with strict=False to skip mismatched head weights
        model_state = instance.state_dict()
        filtered = {
            k: v
            for k, v in mapped_state_dict.items()
            if k in model_state and v.shape == model_state[k].shape
        }
        instance.load_state_dict(filtered, strict=False)
        instance.eval()

        logger.info(
            f"Loaded MOMENT model from {pretrained_model_name_or_path} "
            f"({len(filtered)}/{len(model_state)} keys matched)"
        )
        return instance

    @property
    def device(self):
        return next(self.parameters()).device
