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

import math
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from typing import Generator, List, Optional, Tuple, Union

import numpy as np
import torch
import torch.nn.functional as F
from einops import rearrange, reduce, repeat
from huggingface_hub import PyTorchModelHubMixin
from jaxtyping import Bool, Float, Int
from torch import nn
from transformers import PreTrainedModel
from transformers.modeling_outputs import ModelOutput

from iotdb.ainode.core.model.moirai2.common.torch_util import (
    packed_causal_attention_mask,
)
from iotdb.ainode.core.model.moirai2.configuration_moirai2 import Moirai2Config
from iotdb.ainode.core.model.moirai2.module.norm import RMSNorm
from iotdb.ainode.core.model.moirai2.module.packed_scaler import (
    PackedNOPScaler,
    PackedStdScaler,
)
from iotdb.ainode.core.model.moirai2.module.position import (
    BinaryAttentionBias,
    QueryKeyProjection,
    RotaryProjection,
)
from iotdb.ainode.core.model.moirai2.module.transformer import TransformerEncoder
from iotdb.ainode.core.model.moirai2.module.ts_embed import ResidualBlock
from iotdb.ainode.core.model.moirai2.transform.imputation import CausalMeanImputation


@dataclass
class Moirai2Output(ModelOutput):
    """
    Output class for Moirai2 model.

    Args:
        predictions: Model predictions of shape (batch, seq_len, num_quantiles * patch_size)
        scaled_target: Scaled target values (only during training)
        loc: Scaling location parameters
        scale: Scaling scale parameters
    """

    predictions: torch.FloatTensor = None
    scaled_target: Optional[torch.FloatTensor] = None
    loc: Optional[torch.FloatTensor] = None
    scale: Optional[torch.FloatTensor] = None


class Moirai2PreTrainedModel(PreTrainedModel):
    """
    An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained models.
    """

    config_class = Moirai2Config
    base_model_prefix = "moirai2"
    supports_gradient_checkpointing = False
    _no_split_modules = ["TransformerEncoderLayer"]

    def _init_weights(self, module):
        """Initialize the weights."""
        if isinstance(module, nn.Linear):
            module.weight.data.normal_(mean=0.0, std=0.02)
            if module.bias is not None:
                module.bias.data.zero_()
        elif isinstance(module, nn.Embedding):
            module.weight.data.normal_(mean=0.0, std=0.02)


class Moirai2Model(Moirai2PreTrainedModel, PyTorchModelHubMixin):
    """
    Core Moirai2 transformer model.

    This model implements a transformer-based architecture for time series forecasting
    with support for patching, scaling, and multi-variate predictions.

    Inherits from PyTorchModelHubMixin to enable loading from HuggingFace Hub
    using the from_pretrained() method, which is compatible with uni2ts pretrained models.
    """

    def __init__(self, config: Moirai2Config = None, **kwargs):
        # Handle both config and kwargs for PyTorchModelHubMixin compatibility
        if config is None:
            config = Moirai2Config(**kwargs)
        super().__init__(config)
        self.config = config

        # Model parameters
        self.d_model = config.d_model
        self.num_layers = config.num_layers
        self.patch_size = config.patch_size
        self.num_predict_token = config.num_predict_token
        self.max_seq_len = config.max_seq_len
        self.scaling = config.scaling
        self.quantile_levels = config.quantile_levels
        self.num_quantiles = len(config.quantile_levels)

        # Scaler
        self.scaler = PackedStdScaler() if self.scaling else PackedNOPScaler()

        # Input projection
        self.in_proj = ResidualBlock(
            input_dims=self.patch_size * 2,
            hidden_dims=self.d_model,
            output_dims=self.d_model,
        )

        # Transformer encoder
        self.encoder = TransformerEncoder(
            self.d_model,
            self.num_layers,
            num_heads=None,
            pre_norm=True,
            attn_dropout_p=config.attn_dropout_p,
            dropout_p=config.dropout_p,
            norm_layer=RMSNorm,
            activation=F.silu,
            use_glu=True,
            use_qk_norm=True,
            var_attn_bias_layer=partial(BinaryAttentionBias),
            time_qk_proj_layer=partial(
                QueryKeyProjection,
                proj_layer=RotaryProjection,
                kwargs=dict(max_len=self.max_seq_len),
                partial_factor=(0.0, 0.5),
            ),
            shared_var_attn_bias=False,
            shared_time_qk_proj=True,
            d_ff=config.d_ff,
        )

        # Output projection
        self.out_proj = ResidualBlock(
            input_dims=self.d_model,
            hidden_dims=self.d_model,
            output_dims=self.num_predict_token * self.num_quantiles * self.patch_size,
        )

        self.post_init()

    def forward(
        self,
        target: Float[torch.Tensor, "*batch seq_len patch"],
        observed_mask: Bool[torch.Tensor, "*batch seq_len patch"],
        sample_id: Int[torch.Tensor, "*batch seq_len"],
        time_id: Int[torch.Tensor, "*batch seq_len"],
        variate_id: Int[torch.Tensor, "*batch seq_len"],
        prediction_mask: Bool[torch.Tensor, "*batch seq_len"],
        training_mode: bool = True,
        return_dict: Optional[bool] = None,
    ) -> Union[Tuple, Moirai2Output]:
        """
        Forward pass of Moirai2Model.

        Args:
            target: Input time series data
            observed_mask: Binary mask for observed values (1=observed, 0=missing)
            sample_id: Sample indices for packed sequences
            time_id: Time step indices
            variate_id: Variable indices
            prediction_mask: Binary mask for prediction horizon (1=predict, 0=context)
            training_mode: Whether in training mode
            return_dict: Whether to return ModelOutput

        Returns:
            Moirai2Output or tuple with predictions
        """
        return_dict = (
            return_dict if return_dict is not None else self.config.use_return_dict
        )

        # Apply scaling
        loc, scale = self.scaler(
            target,
            observed_mask * ~prediction_mask.unsqueeze(-1),
            sample_id,
            variate_id,
        )
        scaled_target = (target - loc) / scale

        # Concatenate scaled values with mask
        input_tokens = torch.cat(
            [scaled_target, observed_mask.to(torch.float32)], dim=-1
        )

        # Project to model dimension
        reprs = self.in_proj(input_tokens)

        # Apply transformer
        reprs = self.encoder(
            reprs,
            packed_causal_attention_mask(sample_id, time_id),
            time_id=time_id,
            var_id=variate_id,
        )

        # Project to output
        preds = self.out_proj(reprs)

        if training_mode:
            scaled_preds = preds
            if not return_dict:
                return (preds, scaled_target)
            return Moirai2Output(
                predictions=scaled_preds,
                scaled_target=scaled_target,
                loc=loc,
                scale=scale,
            )
        else:
            # Rescale predictions
            preds = preds * scale + loc
            if not return_dict:
                return (preds,)
            return Moirai2Output(
                predictions=preds,
                loc=loc,
                scale=scale,
            )


class Moirai2ForPrediction(Moirai2PreTrainedModel):
    """
    Moirai2 model for time series prediction.

    This class wraps Moirai2Model and provides high-level prediction interfaces,
    including support for quantile forecasting and various input formats.
    """

    def __init__(
        self,
        config: Moirai2Config,
        prediction_length: int = 96,
        target_dim: int = 1,
        feat_dynamic_real_dim: int = 0,
        past_feat_dynamic_real_dim: int = 0,
        context_length: int = 512,
    ):
        super().__init__(config)
        self.config = config

        # Hyperparameters
        self.prediction_length = prediction_length
        self.target_dim = target_dim
        self.feat_dynamic_real_dim = feat_dynamic_real_dim
        self.past_feat_dynamic_real_dim = past_feat_dynamic_real_dim
        self.context_length = context_length

        # Core model
        self.model = Moirai2Model(config)

        self.post_init()

    @classmethod
    def from_pretrained(
        cls,
        pretrained_model_name_or_path: str,
        prediction_length: int = 96,
        target_dim: int = 1,
        feat_dynamic_real_dim: int = 0,
        past_feat_dynamic_real_dim: int = 0,
        context_length: int = 512,
        **kwargs,
    ):
        """
        Load a pretrained Moirai2ForPrediction model.

        This method handles the weight loading from uni2ts pretrained models,
        which have a different weight key structure (without 'model.' prefix).

        Args:
            pretrained_model_name_or_path: Path or HuggingFace repo ID
            prediction_length: Length of forecast horizon
            target_dim: Number of target variables
            feat_dynamic_real_dim: Dimension of future dynamic features
            past_feat_dynamic_real_dim: Dimension of past dynamic features
            context_length: Length of context window
            **kwargs: Additional arguments for loading

        Returns:
            Moirai2ForPrediction instance with loaded weights
        """
        import os
        from pathlib import Path

        # Load config
        config = Moirai2Config.from_pretrained(pretrained_model_name_or_path, **kwargs)

        # Create instance normally (this will create a randomly initialized core model)
        instance = cls(
            config=config,
            prediction_length=prediction_length,
            target_dim=target_dim,
            feat_dynamic_real_dim=feat_dynamic_real_dim,
            past_feat_dynamic_real_dim=past_feat_dynamic_real_dim,
            context_length=context_length,
        )

        # Now load weights into the core model
        # Try to find the weight file
        model_dir = Path(pretrained_model_name_or_path)
        weight_file = None

        # Check for various weight file formats
        for filename in ["model.safetensors", "pytorch_model.bin", "model.bin"]:
            candidate = model_dir / filename
            if candidate.exists():
                weight_file = candidate
                break

        if weight_file is None:
            warnings.warn(
                f"No weight file found in {pretrained_model_name_or_path}. "
                f"Model will use randomly initialized weights."
            )
            return instance

        # Load the state dict
        try:
            if str(weight_file).endswith(".safetensors"):
                # Use safetensors if available
                try:
                    from safetensors.torch import load_file

                    state_dict = load_file(str(weight_file))
                except ImportError:
                    # Fallback to torch.load (won't work for safetensors)
                    warnings.warn(
                        "safetensors not available, trying torch.load. "
                        "Please install safetensors for better compatibility."
                    )
                    state_dict = torch.load(str(weight_file), map_location="cpu")
            else:
                state_dict = torch.load(str(weight_file), map_location="cpu")

            # Load weights into the core model
            # The uni2ts weights don't have 'model.' prefix, load directly
            missing_keys, unexpected_keys = instance.model.load_state_dict(
                state_dict, strict=False
            )

            if missing_keys:
                warnings.warn(
                    f"Missing keys when loading pretrained weights: {missing_keys[:5]}..."
                )
            if unexpected_keys:
                warnings.warn(
                    f"Unexpected keys when loading pretrained weights: {unexpected_keys[:5]}..."
                )

            if not missing_keys:
                # Successfully loaded all weights
                pass
            else:
                warnings.warn(
                    f"Some weights were not loaded. Model may not work correctly."
                )

        except Exception as e:
            warnings.warn(
                f"Failed to load weights from {weight_file}: {e}\n"
                f"Model will use randomly initialized weights."
            )

        return instance

    def set_decoder(self, decoder):
        """Set the decoder model (for compatibility with TSGenerationMixin)."""
        self.model = decoder

    def get_decoder(self):
        """Get the decoder model (for compatibility with TSGenerationMixin)."""
        return self.model

    @property
    def past_length(self) -> int:
        """Get the context length."""
        return self.context_length

    @property
    def max_patch_size(self) -> int:
        """Get the maximum patch size."""
        return self.model.patch_size

    def context_token_length(self, patch_size: int) -> int:
        """Calculate the number of tokens in the context."""
        return math.ceil(self.context_length / patch_size)

    def prediction_token_length(self, patch_size: int) -> int:
        """Calculate the number of tokens in the prediction."""
        return math.ceil(self.prediction_length / patch_size)

    @contextmanager
    def hparams_context(
        self,
        prediction_length: Optional[int] = None,
        target_dim: Optional[int] = None,
        feat_dynamic_real_dim: Optional[int] = None,
        past_feat_dynamic_real_dim: Optional[int] = None,
        context_length: Optional[int] = None,
    ) -> Generator["Moirai2ForPrediction", None, None]:
        """
        Context manager for temporarily changing hyperparameters.
        """
        kwargs = {
            "prediction_length": prediction_length,
            "target_dim": target_dim,
            "feat_dynamic_real_dim": feat_dynamic_real_dim,
            "past_feat_dynamic_real_dim": past_feat_dynamic_real_dim,
            "context_length": context_length,
        }

        # Save old values
        old_values = {
            "prediction_length": self.prediction_length,
            "target_dim": self.target_dim,
            "feat_dynamic_real_dim": self.feat_dynamic_real_dim,
            "past_feat_dynamic_real_dim": self.past_feat_dynamic_real_dim,
            "context_length": self.context_length,
        }

        # Set new values
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)

        try:
            yield self
        finally:
            # Restore old values
            for key, value in old_values.items():
                setattr(self, key, value)

    def forward(
        self,
        past_target: Float[torch.Tensor, "batch past_time tgt"],
        past_observed_target: Bool[torch.Tensor, "batch past_time tgt"],
        past_is_pad: Bool[torch.Tensor, "batch past_time"],
        feat_dynamic_real: Optional[Float[torch.Tensor, "batch time feat"]] = None,
        observed_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch time feat"]
        ] = None,
        past_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch past_time past_feat"]
        ] = None,
        past_observed_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch past_time past_feat"]
        ] = None,
        return_dict: Optional[bool] = None,
    ) -> Float[torch.Tensor, "batch num_quantiles future_time *tgt"]:
        """
        Forward pass for prediction.

        Args:
            past_target: Historical target values
            past_observed_target: Mask for observed target values
            past_is_pad: Mask for padding in the past
            feat_dynamic_real: Future dynamic features (optional)
            observed_feat_dynamic_real: Mask for dynamic features (optional)
            past_feat_dynamic_real: Past dynamic features (optional)
            past_observed_feat_dynamic_real: Mask for past dynamic features (optional)
            return_dict: Whether to return ModelOutput

        Returns:
            Predictions of shape (batch, num_quantiles, prediction_length, target_dim)
        """
        # Convert inputs to model format
        (
            target,
            observed_mask,
            sample_id,
            time_id,
            variate_id,
            prediction_mask,
        ) = self._convert(
            self.model.patch_size,
            past_target,
            past_observed_target,
            past_is_pad,
            feat_dynamic_real=feat_dynamic_real,
            observed_feat_dynamic_real=observed_feat_dynamic_real,
            past_feat_dynamic_real=past_feat_dynamic_real,
            past_observed_feat_dynamic_real=past_observed_feat_dynamic_real,
        )

        per_var_context_token = self.context_token_length(self.model.patch_size)
        total_context_token = self.target_dim * per_var_context_token
        per_var_predict_token = self.prediction_token_length(self.model.patch_size)
        total_predict_token = self.target_dim * per_var_predict_token

        # Initialize prediction tensor
        pred_index = torch.arange(
            start=per_var_context_token - 1,
            end=total_context_token,
            step=per_var_context_token,
        )
        assign_index = torch.arange(
            start=total_context_token,
            end=total_context_token + total_predict_token,
            step=per_var_predict_token,
        )

        quantile_prediction = repeat(
            target,
            "... patch_size -> ... num_quantiles patch_size",
            num_quantiles=self.model.num_quantiles,
            patch_size=self.model.patch_size,
        ).clone()

        # Get model predictions
        outputs = self.model(
            target,
            observed_mask,
            sample_id,
            time_id,
            variate_id,
            prediction_mask,
            training_mode=False,
            return_dict=True,
        )
        preds = outputs.predictions

        # Process predictions
        if per_var_predict_token <= self.model.num_predict_token:
            # Single-step prediction
            preds, adjusted_assign_index = self._structure_multi_predict(
                per_var_predict_token,
                pred_index,
                assign_index,
                preds,
            )
            quantile_prediction[..., adjusted_assign_index, :, :] = preds
            return self._format_preds(
                self.model.num_quantiles,
                self.model.patch_size,
                quantile_prediction,
                self.target_dim,
            )
        else:
            # Multi-step autoregressive prediction
            return self._autoregressive_predict(
                target,
                observed_mask,
                sample_id,
                time_id,
                variate_id,
                prediction_mask,
                quantile_prediction,
                preds,
                pred_index,
                assign_index,
                per_var_predict_token,
            )

    def _structure_multi_predict(
        self,
        per_var_predict_token: int,
        pred_index: torch.Tensor,
        assign_index: torch.Tensor,
        preds: torch.Tensor,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Structure predictions for multiple prediction tokens."""
        preds = rearrange(
            preds,
            "... (predict_token num_quantiles patch_size) -> ... predict_token num_quantiles patch_size",
            predict_token=self.model.num_predict_token,
            num_quantiles=self.model.num_quantiles,
            patch_size=self.model.patch_size,
        )
        preds = rearrange(
            preds[..., pred_index, :per_var_predict_token, :, :],
            "... pred_index predict_token num_quantiles patch_size -> ... (pred_index predict_token) num_quantiles patch_size",
        )
        adjusted_assign_index = torch.cat(
            [
                torch.arange(start=idx, end=idx + per_var_predict_token)
                for idx in assign_index
            ]
        )
        return preds, adjusted_assign_index

    def _autoregressive_predict(
        self,
        target: torch.Tensor,
        observed_mask: torch.Tensor,
        sample_id: torch.Tensor,
        time_id: torch.Tensor,
        variate_id: torch.Tensor,
        prediction_mask: torch.Tensor,
        quantile_prediction: torch.Tensor,
        preds: torch.Tensor,
        pred_index: torch.Tensor,
        assign_index: torch.Tensor,
        per_var_predict_token: int,
    ) -> torch.Tensor:
        """Perform autoregressive prediction for long horizons."""
        # Expand tensors for quantiles
        expand_target = repeat(
            target,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()
        expand_prediction_mask = repeat(
            prediction_mask,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()
        expand_observed_mask = repeat(
            observed_mask,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()
        expand_sample_id = repeat(
            sample_id,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()
        expand_time_id = repeat(
            time_id,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()
        expand_variate_id = repeat(
            variate_id,
            "batch_size ... -> batch_size num_quantiles ...",
            num_quantiles=self.model.num_quantiles,
            batch_size=target.shape[0],
        ).clone()

        # First prediction step
        preds, adjusted_assign_index = self._structure_multi_predict(
            self.model.num_predict_token,
            pred_index,
            assign_index,
            preds,
        )
        quantile_prediction[..., adjusted_assign_index, :, :] = preds

        expand_target[..., adjusted_assign_index, :] = rearrange(
            preds,
            "... predict_token num_quantiles patch_size -> ... num_quantiles predict_token patch_size",
            num_quantiles=self.model.num_quantiles,
            patch_size=self.model.patch_size,
            predict_token=self.model.num_predict_token,
        )
        expand_prediction_mask[..., adjusted_assign_index] = False

        # Remaining steps
        remain_step = per_var_predict_token - self.model.num_predict_token
        while remain_step > 0:
            outputs = self.model(
                expand_target,
                expand_observed_mask,
                expand_sample_id,
                expand_time_id,
                expand_variate_id,
                expand_prediction_mask,
                training_mode=False,
                return_dict=True,
            )
            preds = outputs.predictions

            pred_index = assign_index + self.model.num_predict_token - 1
            assign_index = pred_index + 1
            preds, adjusted_assign_index = self._structure_multi_predict(
                (
                    self.model.num_predict_token
                    if remain_step > self.model.num_predict_token
                    else remain_step
                ),
                pred_index,
                assign_index,
                preds,
            )

            # Compute quantiles
            quantile_prediction_next_step = rearrange(
                preds,
                "... num_quantiles_prev pred_index num_quantiles patch_size -> ... pred_index (num_quantiles_prev num_quantiles) patch_size",
                num_quantiles=self.model.num_quantiles,
                patch_size=self.model.patch_size,
            )
            quantile_prediction_next_step = torch.quantile(
                quantile_prediction_next_step,
                torch.tensor(
                    self.model.quantile_levels,
                    device=target.device,
                    dtype=torch.float32,
                ),
                dim=-2,
            )
            quantile_prediction[..., adjusted_assign_index, :, :] = rearrange(
                quantile_prediction_next_step,
                "num_quantiles ... patch_size -> ... num_quantiles patch_size",
            )

            expand_target[..., adjusted_assign_index, :] = rearrange(
                quantile_prediction_next_step,
                "num_quantiles batch_size predict_token patch_size -> batch_size num_quantiles predict_token patch_size",
                num_quantiles=self.model.num_quantiles,
                patch_size=self.model.patch_size,
                predict_token=len(adjusted_assign_index),
            )
            expand_prediction_mask[..., adjusted_assign_index] = False

            remain_step -= self.model.num_predict_token

        return self._format_preds(
            self.model.num_quantiles,
            self.model.patch_size,
            quantile_prediction,
            self.target_dim,
        )

    @torch.no_grad()
    def predict(
        self,
        past_target: List[Float[np.ndarray, "past_time tgt"]],
        feat_dynamic_real: Optional[List[Float[np.ndarray, "time feat"]]] = None,
        past_feat_dynamic_real: Optional[
            List[Float[np.ndarray, "past_time feat"]]
        ] = None,
    ) -> Float[np.ndarray, "batch num_quantiles future_time *tgt"]:
        """
        High-level prediction interface accepting list of numpy arrays.

        Args:
            past_target: List of historical time series (numpy arrays)
            feat_dynamic_real: List of future dynamic features (optional)
            past_feat_dynamic_real: List of past dynamic features (optional)

        Returns:
            Predictions as numpy array of shape (batch, num_quantiles, prediction_length, target_dim)
        """
        # Prepare data
        data_entry = {
            "past_target": past_target,
            "feat_dynamic_real": feat_dynamic_real,
            "past_feat_dynamic_real": past_feat_dynamic_real,
        }

        # Create observed masks
        data_entry["past_observed_target"] = [~np.isnan(x) for x in past_target]
        if feat_dynamic_real:
            data_entry["observed_feat_dynamic_real"] = [
                ~np.isnan(x) for x in feat_dynamic_real
            ]
        else:
            data_entry["observed_feat_dynamic_real"] = None

        if past_feat_dynamic_real:
            data_entry["past_observed_feat_dynamic_real"] = [
                ~np.isnan(x) for x in past_feat_dynamic_real
            ]
        else:
            data_entry["past_observed_feat_dynamic_real"] = None

        # Impute missing values
        impute = CausalMeanImputation()

        def process_sample(sample):
            arr = np.asarray(sample)
            if arr.ndim == 1:
                arr = arr[:, np.newaxis]
            if np.issubdtype(arr.dtype, np.number) and np.isnan(arr).any():
                arr = impute(arr)
            return arr

        for key, value in data_entry.items():
            if value is not None:
                data_entry[key] = [process_sample(sample) for sample in value]

        # Create padding mask
        data_entry["past_is_pad"] = np.zeros(
            (len(data_entry["past_target"]), self.context_length), dtype=bool
        )

        # Pad or slice to context length
        for key in data_entry:
            if data_entry[key] is not None and isinstance(data_entry[key], list):
                for idx in range(len(data_entry[key])):
                    if data_entry[key][idx].shape[0] > self.context_length:
                        data_entry[key][idx] = data_entry[key][idx][
                            -self.context_length :, :
                        ]
                    else:
                        pad_length = self.context_length - data_entry[key][idx].shape[0]
                        pad_block = np.full(
                            (pad_length, data_entry[key][idx].shape[1]),
                            data_entry[key][idx][0],
                            dtype=data_entry[key][idx].dtype,
                        )
                        data_entry[key][idx] = np.concatenate(
                            [pad_block, data_entry[key][idx]], axis=0
                        )
                        if key == "past_target":
                            data_entry["past_is_pad"][idx, :pad_length] = True

        # Convert to tensors
        device = next(self.parameters()).device
        for k in ["past_target", "feat_dynamic_real", "past_feat_dynamic_real"]:
            if data_entry[k] is not None:
                data_entry[k] = torch.tensor(
                    np.array(data_entry[k]), device=device, dtype=torch.float32
                )

        for k in [
            "past_observed_target",
            "observed_feat_dynamic_real",
            "past_observed_feat_dynamic_real",
            "past_is_pad",
        ]:
            if data_entry[k] is not None:
                data_entry[k] = torch.tensor(
                    np.array(data_entry[k]), device=device, dtype=torch.bool
                )

        # Get predictions
        predictions = self(**data_entry).detach().cpu().numpy()
        return predictions

    @staticmethod
    def _patched_seq_pad(
        patch_size: int,
        x: torch.Tensor,
        dim: int,
        left: bool = True,
        value: Optional[float] = None,
    ) -> torch.Tensor:
        """Pad sequence to be divisible by patch_size."""
        if dim >= 0:
            dim = -x.ndim + dim
        pad_length = -x.size(dim) % patch_size
        if left:
            pad = (pad_length, 0)
        else:
            pad = (0, pad_length)
        pad = (0, 0) * (abs(dim) - 1) + pad
        return torch.nn.functional.pad(x, pad, value=value)

    def _generate_time_id(
        self,
        patch_size: int,
        past_observed_target: Bool[torch.Tensor, "batch past_seq tgt"],
    ) -> Tuple[
        Int[torch.Tensor, "batch past_token"], Int[torch.Tensor, "batch future_token"]
    ]:
        """Generate time IDs for past and future sequences."""
        past_seq_id = reduce(
            self._patched_seq_pad(patch_size, past_observed_target, -2, left=True),
            "... (seq patch) dim -> ... seq",
            "max",
            patch=patch_size,
        )
        past_seq_id = torch.clamp(
            past_seq_id.cummax(dim=-1).values.cumsum(dim=-1) - 1, min=0
        )

        batch_shape = " ".join(map(str, past_observed_target.shape[:-2]))
        future_seq_id = (
            repeat(
                torch.arange(
                    self.prediction_token_length(patch_size),
                    device=past_observed_target.device,
                ),
                f"prediction -> {batch_shape} prediction",
            )
            + past_seq_id.max(dim=-1, keepdim=True).values
            + 1
        )
        return past_seq_id, future_seq_id

    def _convert(
        self,
        patch_size: int,
        past_target: Float[torch.Tensor, "batch past_time tgt"],
        past_observed_target: Bool[torch.Tensor, "batch past_time tgt"],
        past_is_pad: Bool[torch.Tensor, "batch past_time"],
        future_target: Optional[Float[torch.Tensor, "batch future_time tgt"]] = None,
        future_observed_target: Optional[
            Bool[torch.Tensor, "batch future_time tgt"]
        ] = None,
        future_is_pad: Optional[Bool[torch.Tensor, "batch future_time"]] = None,
        feat_dynamic_real: Optional[Float[torch.Tensor, "batch time feat"]] = None,
        observed_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch time feat"]
        ] = None,
        past_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch past_time past_feat"]
        ] = None,
        past_observed_feat_dynamic_real: Optional[
            Float[torch.Tensor, "batch past_time past_feat"]
        ] = None,
    ) -> Tuple[
        Float[torch.Tensor, "batch combine_seq patch"],
        Bool[torch.Tensor, "batch combine_seq patch"],
        Int[torch.Tensor, "batch combine_seq"],
        Int[torch.Tensor, "batch combine_seq"],
        Int[torch.Tensor, "batch combine_seq"],
        Bool[torch.Tensor, "batch combine_seq"],
    ]:
        """
        Convert input tensors to packed format for the model.

        Returns:
            Tuple of (target, observed_mask, sample_id, time_id, variate_id, prediction_mask)
        """
        batch_shape = past_target.shape[:-2]
        device = past_target.device

        target = []
        observed_mask = []
        sample_id = []
        time_id = []
        variate_id = []
        prediction_mask = []
        dim_count = 0

        past_seq_id, future_seq_id = self._generate_time_id(
            patch_size, past_observed_target
        )

        # Process target variable
        if future_target is None:
            future_target = torch.zeros(
                batch_shape + (self.prediction_length, past_target.shape[-1]),
                dtype=past_target.dtype,
                device=device,
            )

        target.extend(
            [
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(patch_size, past_target, -2, left=True),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                ),
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(
                            patch_size, future_target, -2, left=False
                        ),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                ),
            ]
        )

        if future_observed_target is None:
            future_observed_target = torch.ones(
                batch_shape + (self.prediction_length, past_observed_target.shape[-1]),
                dtype=torch.bool,
                device=device,
            )

        observed_mask.extend(
            [
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(
                            patch_size, past_observed_target, -2, left=True
                        ),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                ),
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(
                            patch_size, future_observed_target, -2, left=False
                        ),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                ),
            ]
        )

        if future_is_pad is None:
            future_is_pad = torch.zeros(
                batch_shape + (self.prediction_length,),
                dtype=torch.long,
                device=device,
            )

        sample_id.extend(
            [
                repeat(
                    reduce(
                        (
                            self._patched_seq_pad(
                                patch_size, past_is_pad, -1, left=True, value=1
                            )
                            == 0
                        ).int(),
                        "... (seq patch) -> ... seq",
                        "max",
                        patch=patch_size,
                    ),
                    "... seq -> ... (dim seq)",
                    dim=past_target.shape[-1],
                ),
                repeat(
                    reduce(
                        (
                            self._patched_seq_pad(
                                patch_size, future_is_pad, -1, left=False, value=1
                            )
                            == 0
                        ).int(),
                        "... (seq patch) -> ... seq",
                        "max",
                        patch=patch_size,
                    ),
                    "... seq -> ... (dim seq)",
                    dim=past_target.shape[-1],
                ),
            ]
        )

        time_id.extend(
            [past_seq_id] * past_target.shape[-1]
            + [future_seq_id] * past_target.shape[-1]
        )

        variate_id.extend(
            [
                repeat(
                    torch.arange(past_target.shape[-1], device=device) + dim_count,
                    f"dim -> {' '.join(map(str, batch_shape))} (dim past)",
                    past=self.context_token_length(patch_size),
                ),
                repeat(
                    torch.arange(past_target.shape[-1], device=device) + dim_count,
                    f"dim -> {' '.join(map(str, batch_shape))} (dim future)",
                    future=self.prediction_token_length(patch_size),
                ),
            ]
        )
        dim_count += past_target.shape[-1]

        prediction_mask.extend(
            [
                torch.zeros(
                    batch_shape
                    + (self.context_token_length(patch_size) * past_target.shape[-1],),
                    dtype=torch.bool,
                    device=device,
                ),
                torch.ones(
                    batch_shape
                    + (
                        self.prediction_token_length(patch_size)
                        * past_target.shape[-1],
                    ),
                    dtype=torch.bool,
                    device=device,
                ),
            ]
        )

        # Process dynamic features if provided
        if feat_dynamic_real is not None:
            if observed_feat_dynamic_real is None:
                raise ValueError(
                    "observed_feat_dynamic_real must be provided if feat_dynamic_real is provided"
                )

            target.extend(
                [
                    torch.nn.functional.pad(
                        rearrange(
                            self._patched_seq_pad(
                                patch_size,
                                feat_dynamic_real[..., : self.context_length, :],
                                -2,
                                left=True,
                            ),
                            "... (seq patch) dim -> ... (dim seq) patch",
                            patch=patch_size,
                        ),
                        (0, 0),
                    ),
                    torch.nn.functional.pad(
                        rearrange(
                            self._patched_seq_pad(
                                patch_size,
                                feat_dynamic_real[..., self.context_length :, :],
                                -2,
                                left=False,
                            ),
                            "... (seq patch) dim -> ... (dim seq) patch",
                            patch=patch_size,
                        ),
                        (0, 0),
                    ),
                ]
            )

            observed_mask.extend(
                [
                    torch.nn.functional.pad(
                        rearrange(
                            self._patched_seq_pad(
                                patch_size,
                                observed_feat_dynamic_real[
                                    ..., : self.context_length, :
                                ],
                                -2,
                                left=True,
                            ),
                            "... (seq patch) dim -> ... (dim seq) patch",
                            patch=patch_size,
                        ),
                        (0, 0),
                    ),
                    torch.nn.functional.pad(
                        rearrange(
                            self._patched_seq_pad(
                                patch_size,
                                observed_feat_dynamic_real[
                                    ..., self.context_length :, :
                                ],
                                -2,
                                left=False,
                            ),
                            "... (seq patch) dim -> ... (dim seq) patch",
                            patch=patch_size,
                        ),
                        (0, 0),
                    ),
                ]
            )

            sample_id.extend(
                [
                    repeat(
                        reduce(
                            (
                                self._patched_seq_pad(
                                    patch_size, past_is_pad, -1, left=True
                                )
                                == 0
                            ).int(),
                            "... (seq patch) -> ... seq",
                            "max",
                            patch=patch_size,
                        ),
                        "... seq -> ... (dim seq)",
                        dim=feat_dynamic_real.shape[-1],
                    ),
                    torch.ones(
                        batch_shape
                        + (
                            self.prediction_token_length(patch_size)
                            * feat_dynamic_real.shape[-1],
                        ),
                        dtype=torch.long,
                        device=device,
                    ),
                ]
            )

            time_id.extend(
                [past_seq_id] * feat_dynamic_real.shape[-1]
                + [future_seq_id] * feat_dynamic_real.shape[-1]
            )

            variate_id.extend(
                [
                    repeat(
                        torch.arange(feat_dynamic_real.shape[-1], device=device)
                        + dim_count,
                        f"dim -> {' '.join(map(str, batch_shape))} (dim past)",
                        past=self.context_token_length(patch_size),
                    ),
                    repeat(
                        torch.arange(feat_dynamic_real.shape[-1], device=device)
                        + dim_count,
                        f"dim -> {' '.join(map(str, batch_shape))} (dim future)",
                        future=self.prediction_token_length(patch_size),
                    ),
                ]
            )
            dim_count += feat_dynamic_real.shape[-1]

            prediction_mask.extend(
                [
                    torch.zeros(
                        batch_shape
                        + (
                            self.context_token_length(patch_size)
                            * feat_dynamic_real.shape[-1],
                        ),
                        dtype=torch.bool,
                        device=device,
                    ),
                    torch.zeros(
                        batch_shape
                        + (
                            self.prediction_token_length(patch_size)
                            * feat_dynamic_real.shape[-1],
                        ),
                        dtype=torch.bool,
                        device=device,
                    ),
                ]
            )

        if past_feat_dynamic_real is not None:
            if past_observed_feat_dynamic_real is None:
                raise ValueError(
                    "past_observed_feat_dynamic_real must be provided if past_feat_dynamic_real is provided"
                )

            target.append(
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(
                            patch_size, past_feat_dynamic_real, -2, left=True
                        ),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                )
            )

            observed_mask.append(
                torch.nn.functional.pad(
                    rearrange(
                        self._patched_seq_pad(
                            patch_size, past_observed_feat_dynamic_real, -2, left=True
                        ),
                        "... (seq patch) dim -> ... (dim seq) patch",
                        patch=patch_size,
                    ),
                    (0, 0),
                )
            )

            sample_id.append(
                repeat(
                    reduce(
                        (
                            self._patched_seq_pad(
                                patch_size, past_is_pad, -1, left=True
                            )
                            == 0
                        ).int(),
                        "... (seq patch) -> ... seq",
                        "max",
                        patch=patch_size,
                    ),
                    "... seq -> ... (dim seq)",
                    dim=past_feat_dynamic_real.shape[-1],
                )
            )

            time_id.extend([past_seq_id] * past_feat_dynamic_real.shape[-1])

            variate_id.append(
                repeat(
                    torch.arange(past_feat_dynamic_real.shape[-1], device=device)
                    + dim_count,
                    f"dim -> {' '.join(map(str, batch_shape))} (dim past)",
                    past=self.context_token_length(patch_size),
                )
            )
            dim_count += past_feat_dynamic_real.shape[-1]

            prediction_mask.append(
                torch.zeros(
                    batch_shape
                    + (
                        self.context_token_length(patch_size)
                        * past_feat_dynamic_real.shape[-1],
                    ),
                    dtype=torch.bool,
                    device=device,
                )
            )

        # Concatenate all components
        target = torch.cat(target, dim=-2)
        observed_mask = torch.cat(observed_mask, dim=-2)
        sample_id = torch.cat(sample_id, dim=-1)
        time_id = torch.cat(time_id, dim=-1)
        variate_id = torch.cat(variate_id, dim=-1)
        prediction_mask = torch.cat(prediction_mask, dim=-1)

        return (
            target,
            observed_mask,
            sample_id,
            time_id,
            variate_id,
            prediction_mask,
        )

    def _format_preds(
        self,
        num_quantiles: int,
        patch_size: int,
        preds: Float[torch.Tensor, "batch combine_seq patch"],
        target_dim: int,
    ) -> Float[torch.Tensor, "batch num_quantiles future_time *tgt"]:
        """Format predictions to the expected output shape."""
        start = target_dim * self.context_token_length(patch_size)
        end = start + target_dim * self.prediction_token_length(patch_size)
        preds = preds[..., start:end, :num_quantiles, :patch_size]
        preds = rearrange(
            preds,
            "... (dim seq) num_quantiles patch -> ... num_quantiles (seq patch) dim",
            dim=target_dim,
        )[..., : self.prediction_length, :]
        return preds.squeeze(-1)
