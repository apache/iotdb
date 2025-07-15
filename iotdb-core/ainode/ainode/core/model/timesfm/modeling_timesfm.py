# modeling_timesfm.py
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

import math
import torch
import os
import torch.nn as nn
import torch.nn.functional as F
import safetensors.torch
from transformers import PreTrainedModel
from transformers.modeling_outputs import BaseModelOutput
from typing import Optional, Union, Sequence, List, Tuple
from dataclasses import dataclass

from ainode.core.log import Logger
from ainode.core.model.timesfm.configuration_timesfm import TimesFmConfig
from ainode.core.model.timesfm.timesfm_generation_mixin import TimesFmGenerationMixin

logger = Logger()

@dataclass
class TimesFmOutput(BaseModelOutput):
    loc: Optional[torch.Tensor] = None
    scale: Optional[torch.Tensor] = None

@dataclass
class TimesFmOutputForPrediction(BaseModelOutput):
    mean_predictions: Optional[torch.Tensor] = None
    full_predictions: Optional[torch.Tensor] = None
    loss: Optional[Union[torch.Tensor, float]] = None

class TimesFmResidualBlock(nn.Module):
    def __init__(self, input_dims, hidden_dims, output_dims):
        super().__init__()
        self.input_dims = input_dims
        self.hidden_dims = hidden_dims
        self.output_dims = output_dims

        self.input_layer = nn.Linear(input_dims, hidden_dims)
        self.activation = nn.SiLU()
        self.output_layer = nn.Linear(hidden_dims, output_dims)
        self.residual_layer = nn.Linear(input_dims, output_dims)

    def forward(self, x):
        hidden = self.input_layer(x)
        hidden = self.activation(hidden)
        output = self.output_layer(hidden)
        residual = self.residual_layer(x)
        return output + residual

class TimesFmRMSNorm(nn.Module):
    def __init__(self, hidden_size, eps=1e-6):
        super().__init__()
        self.weight = nn.Parameter(torch.ones(hidden_size))
        self.variance_epsilon = eps

    def forward(self, hidden_states):
        input_dtype = hidden_states.dtype
        hidden_states = hidden_states.to(torch.float32)
        variance = hidden_states.pow(2).mean(-1, keepdim=True)
        hidden_states = hidden_states * torch.rsqrt(variance + self.variance_epsilon)
        return self.weight * hidden_states.to(input_dtype)

class TimesFmMLP(nn.Module):
    def __init__(self, config: TimesFmConfig):
        super().__init__()
        hidden_size = config.hidden_size
        intermediate_size = config.intermediate_size

        self.gate_proj = nn.Linear(hidden_size, intermediate_size)
        self.down_proj = nn.Linear(intermediate_size, hidden_size)
        self.layer_norm = nn.LayerNorm(normalized_shape=hidden_size, eps=1e-6)

    def forward(self, x, paddings=None):
        gate_inp = self.layer_norm(x)
        gate = self.gate_proj(gate_inp)
        gate = F.relu(gate)
        outputs = self.down_proj(gate)
        if paddings is not None:
            outputs = outputs * (1.0 - paddings[:, :, None])
        return outputs + x

class TimesFmPositionalEmbedding(nn.Module):
    def __init__(self, config: TimesFmConfig):
        super().__init__()
        min_timescale = config.min_timescale
        max_timescale = config.max_timescale
        self.embedding_dims = config.hidden_size

        num_timescales = self.embedding_dims // 2
        log_timescale_increment = math.log(float(max_timescale) / float(min_timescale)) / max(num_timescales - 1, 1)
        self.register_buffer(
            "inv_timescales",
            min_timescale * torch.exp(torch.arange(num_timescales, dtype=torch.float32) * -log_timescale_increment),
        )

    def forward(self, seq_length=None, position=None):
        if position is None and seq_length is None:
            raise ValueError("Either position or seq_length must be provided")

        if position is None:
            position = torch.arange(seq_length, dtype=torch.float32, device=self.inv_timescales.device).unsqueeze(0)
        elif position.ndim != 2:
            raise ValueError(f"position must be 2-dimensional, got shape {position.shape}")

        scaled_time = position.view(*position.shape, 1) * self.inv_timescales.view(1, 1, -1)
        signal = torch.cat([torch.sin(scaled_time), torch.cos(scaled_time)], dim=2)
        signal = F.pad(signal, (0, 0, 0, self.embedding_dims % 2))
        return signal

class TimesFmAttention(nn.Module):
    def __init__(self, config: TimesFmConfig, layer_idx: int):
        super().__init__()
        self.config = config
        self.is_causal = True
        self.attention_dropout = config.attention_dropout
        self.layer_idx = layer_idx

        self.num_heads = config.num_attention_heads
        self.hidden_size = config.hidden_size
        self.head_dim = config.head_dim

        self.q_size = self.num_heads * self.head_dim
        self.kv_size = self.num_heads * self.head_dim
        self.scaling = nn.Parameter(torch.empty((self.head_dim,)))

        self.q_proj = nn.Linear(self.hidden_size, self.num_heads * self.head_dim)
        self.k_proj = nn.Linear(self.hidden_size, self.num_heads * self.head_dim)
        self.v_proj = nn.Linear(self.hidden_size, self.num_heads * self.head_dim)
        self.o_proj = nn.Linear(self.num_heads * self.head_dim, self.hidden_size)

    def _scale_query(self, query: torch.Tensor) -> torch.Tensor:
        scale = F.softplus(self.scaling).mul(1.442695041 / math.sqrt(self.head_dim))
        return query * scale[None, None, None, :]

    def forward(self, hidden_states: torch.Tensor, attention_mask: Optional[torch.Tensor] = None):
        input_shape = hidden_states.shape[:-1]
        hidden_shape = (*input_shape, -1, self.head_dim)

        query_states = self.q_proj(hidden_states).view(hidden_shape).transpose(1, 2)
        query_states = self._scale_query(query_states)
        key_states = self.k_proj(hidden_states).view(hidden_shape).transpose(1, 2)
        value_states = self.v_proj(hidden_states).view(hidden_shape).transpose(1, 2)

        attn_weights = torch.matmul(query_states, key_states.transpose(2, 3))
        if attention_mask is not None:
            causal_mask = attention_mask[:, :, :, : key_states.shape[-2]]
            attn_weights = attn_weights + causal_mask

        attn_weights = F.softmax(attn_weights, dim=-1, dtype=torch.float32).to(query_states.dtype)
        attn_weights = F.dropout(attn_weights, p=self.attention_dropout, training=self.training)
        attn_output = torch.matmul(attn_weights, value_states)
        
        attn_output = attn_output.transpose(1, 2).contiguous()
        attn_output = attn_output.reshape(*input_shape, -1).contiguous()
        attn_output = self.o_proj(attn_output)
        
        return attn_output, attn_weights

class TimesFmDecoderLayer(nn.Module):
    def __init__(self, config: TimesFmConfig, layer_idx: int):
        super().__init__()
        self.self_attn = TimesFmAttention(config, layer_idx=layer_idx)
        self.mlp = TimesFmMLP(config)
        self.input_layernorm = TimesFmRMSNorm(config.hidden_size, eps=config.rms_norm_eps)

    def forward(self, hidden_states: torch.Tensor, attention_mask: torch.Tensor, paddings: torch.Tensor):
        # Self Attention
        residual = hidden_states
        hidden_states = self.input_layernorm(hidden_states)
        hidden_states, scores = self.self_attn(hidden_states=hidden_states, attention_mask=attention_mask)
        hidden_states = residual + hidden_states

        # MLP
        hidden_states = self.mlp(hidden_states, paddings=paddings)

        return scores, hidden_states

class TimesFmPreTrainedModel(PreTrainedModel):
    config_class = TimesFmConfig
    base_model_prefix = "timesfm"
    main_input_name = "past_values"
    
    def _init_weights(self, module):
        if isinstance(module, nn.Linear):
            module.weight.data.normal_(mean=0, std=self.config.initializer_range)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
        elif isinstance(module, TimesFmAttention):
            nn.init.ones_(module.scaling)

    config_class = TimesFmConfig
    base_model_prefix = "timesfm"
    main_input_name = "past_values"
    supports_gradient_checkpointing = True
    _no_split_modules = ["TimesFmDecoderLayer"]
    _skip_keys_device_placement = ["past_key_values"]
    _supports_flash_attn_2 = False
    _supports_sdpa = True
    _supports_cache_class = False
    
    def _init_weights(self, module):
        if isinstance(module, nn.Linear):
            module.weight.data.normal_(mean=0, std=self.config.initializer_range)
            if module.bias is not None:
                nn.init.zeros_(module.bias)
        elif isinstance(module, nn.Embedding):
            module.weight.data.normal_(mean=0, std=self.config.initializer_range)
            if module.padding_idx is not None:
                module.weight.data[module.padding_idx].zero_()
        elif isinstance(module, TimesFmAttention):
            nn.init.ones_(module.scaling)
        elif isinstance(module, TimesFmRMSNorm):
            nn.init.ones_(module.weight)


class TimesFmModel(TimesFmPreTrainedModel):
    def __init__(self, config: TimesFmConfig):
        super().__init__(config)
        self.config = config
        
        self.input_ff_layer = TimesFmResidualBlock(
            input_dims=2 * config.patch_length,
            output_dims=config.hidden_size,
            hidden_dims=config.intermediate_size,
        )
        self.freq_emb = nn.Embedding(num_embeddings=config.freq_size, embedding_dim=config.hidden_size)
        self.layers = nn.ModuleList(
            [TimesFmDecoderLayer(config, layer_idx) for layer_idx in range(config.num_hidden_layers)]
        )
        if self.config.use_positional_embedding:
            self.position_emb = TimesFmPositionalEmbedding(config=config)

        self.post_init()

    def forward(self, past_values: torch.Tensor, past_values_padding: Optional[torch.LongTensor] = None, freq: Optional[torch.Tensor] = None):
        if past_values_padding is None:
            past_values_padding = torch.zeros_like(past_values, dtype=torch.long)
        
        if freq is None:
            batch_size = past_values.shape[0]
            freq = torch.zeros(batch_size, dtype=torch.long, device=past_values.device)
        
        bsize = past_values.shape[0]
        seq_len = past_values.shape[1]
        
        if seq_len % self.config.patch_length != 0:
            pad_len = self.config.patch_length - (seq_len % self.config.patch_length)
            past_values = torch.cat([
                torch.full((bsize, pad_len), self.config.pad_val, 
                        dtype=past_values.dtype, device=past_values.device),
                past_values
            ], dim=1)
            past_values_padding = torch.cat([
                torch.ones((bsize, pad_len), dtype=past_values_padding.dtype, device=past_values_padding.device),
                past_values_padding
            ], dim=1)
        
        seq_len = past_values.shape[1]
        num_patches = seq_len // self.config.patch_length
        
        patched_inputs = past_values.view(bsize, num_patches, self.config.patch_length)
        patched_pads = past_values_padding.view(bsize, num_patches, self.config.patch_length)
        
        patched_inputs = torch.where(
            torch.abs(patched_pads - 1.0) < self.config.tolerance,
            torch.tensor(0.0, dtype=patched_inputs.dtype, device=patched_inputs.device),
            patched_inputs,
        )
        patched_pads = torch.where(
            torch.abs(patched_inputs - self.config.pad_val) < self.config.tolerance,
            torch.tensor(1.0, dtype=patched_pads.dtype, device=patched_pads.device),
            patched_pads,
        )
        
        patched_inputs, stats = self._forward_transform(patched_inputs, patched_pads)
        patched_inputs = patched_inputs * (1.0 - patched_pads)
        
        concat_inputs = torch.cat([patched_inputs, patched_pads], dim=-1)
        
        model_input = self.input_ff_layer(concat_inputs)

        patched_padding = torch.min(patched_pads, dim=-1)[0]
        
        if self.config.use_positional_embedding:
            pos_emb = self.position_emb(model_input.shape[1])
            
            if pos_emb.shape[0] != model_input.shape[0]:
                pos_emb = pos_emb.expand(model_input.shape[0], -1, -1)
            
            model_input += pos_emb

        f_emb = self.freq_emb(freq)  # (batch_size, hidden_size)
        f_emb_expanded = f_emb.unsqueeze(1)  # (batch_size, 1, hidden_size)
        f_emb_broadcasted = f_emb_expanded.expand(-1, model_input.shape[1], -1)
        model_input += f_emb_broadcasted
        hidden_states = model_input
        attention_mask = self._prepare_4d_attention_mask(
            attention_mask=patched_padding,
            sequence_length=hidden_states.shape[1],
            dtype=hidden_states.dtype,
            device=hidden_states.device,
        )
        for i, layer in enumerate(self.layers):
            scores, hidden_states = layer(
                hidden_states=hidden_states,
                attention_mask=attention_mask,
                paddings=patched_padding,
            )
        return TimesFmOutput(
            last_hidden_state=hidden_states,
            loc=stats[0],
            scale=stats[1],
        )

    def _forward_transform(self, inputs: torch.Tensor, patched_pads: torch.Tensor):
        mu, sigma = self._timesfm_masked_mean_std(inputs, patched_pads)
        sigma = torch.where(
            sigma < self.config.tolerance,
            torch.tensor(1.0, dtype=sigma.dtype, device=sigma.device),
            sigma,
        )
        
        outputs = (inputs - mu[:, None, None]) / sigma[:, None, None]
        outputs = torch.where(
            torch.abs(inputs - self.config.pad_val) < self.config.tolerance,
            torch.tensor(self.config.pad_val, dtype=outputs.dtype, device=outputs.device),
            outputs,
        )
        return outputs, (mu, sigma)

    @staticmethod
    def _timesfm_masked_mean_std(inputs: torch.Tensor, padding: torch.Tensor):
        def _get_patch_index(arr: torch.Tensor):
            indices = torch.argmax((arr >= 3).to(torch.int32), dim=1)
            row_sum = (arr >= 3).to(torch.int32).sum(dim=1)
            return torch.where(row_sum == 0, arr.shape[1] - 1, indices)

        pad_sum = torch.sum(1 - padding, dim=2)
        patch_indices = _get_patch_index(pad_sum)
        bidxs = torch.arange(inputs.shape[0])

        arr = inputs[bidxs, patch_indices, :]
        pad = padding[bidxs, patch_indices, :]

        mask = 1 - pad
        num_valid_elements = torch.sum(mask, dim=1)
        num_valid_elements = torch.where(
            num_valid_elements == 0,
            torch.tensor(1, dtype=num_valid_elements.dtype, device=num_valid_elements.device),
            num_valid_elements,
        )

        masked_sum = torch.sum(arr * mask, dim=1)
        masked_squared_sum = torch.sum((arr * mask) ** 2, dim=1)

        masked_mean = masked_sum / num_valid_elements
        masked_var = masked_squared_sum / num_valid_elements - masked_mean**2
        masked_var = torch.where(
            masked_var < 0.0,
            torch.tensor(0.0, dtype=masked_var.dtype, device=masked_var.device),
            masked_var,
        )
        masked_std = torch.sqrt(masked_var)

        return masked_mean, masked_std

    @staticmethod
    def _prepare_4d_attention_mask(attention_mask, sequence_length, dtype, device):
        min_value = torch.finfo(dtype).min if dtype.is_floating_point else torch.iinfo(dtype).min
        
        if attention_mask is not None:
            attention_mask = attention_mask.view(attention_mask.shape[0], 1, 1, -1)
            attention_mask = attention_mask * min_value

        causal_mask = torch.triu(
            torch.ones((sequence_length, sequence_length), dtype=dtype, device=device) * min_value,
            diagonal=1,
        )
        causal_mask = causal_mask.view(1, 1, sequence_length, sequence_length)

        if attention_mask is not None:
            attention_mask = torch.minimum(attention_mask, causal_mask)
        else:
            attention_mask = causal_mask

        return attention_mask
    
class TimesFmForPrediction(TimesFmPreTrainedModel, TimesFmGenerationMixin):  
    def __init__(self, config: TimesFmConfig):
        super().__init__(config)
        self.config = config
        self.context_len = config.context_length
        self.horizon_len = config.horizon_length
        
        self.decoder = TimesFmModel(config)
        
        self.horizon_ff_layer = TimesFmResidualBlock(
            input_dims=config.hidden_size,
            output_dims=config.horizon_length * (1 + len(config.quantiles)),
            hidden_dims=config.intermediate_size,
        )
        
        self.post_init()

    @classmethod
    def from_pretrained(
        cls,
        pretrained_model_name_or_path: Optional[Union[str, os.PathLike]] = None,
        *model_args,
        config: Optional[Union[TimesFmConfig, str, os.PathLike]] = None,
        cache_dir: Optional[Union[str, os.PathLike]] = None,
        ignore_mismatched_sizes: bool = False,
        force_download: bool = False,
        local_files_only: bool = False,
        token: Optional[Union[str, bool]] = None,
        revision: str = "main",
        use_safetensors: bool = None,
        **kwargs,
    ):
        model_id_mapping = {
            "google/timesfm-1.0-200m": "google/timesfm-1.0-200m",
            "timesfm-1.0-200m": "google/timesfm-1.0-200m",
            "timesfm": "google/timesfm-1.0-200m",
        }
        
        if pretrained_model_name_or_path in model_id_mapping:
            pretrained_model_name_or_path = model_id_mapping[pretrained_model_name_or_path]
        
        return super().from_pretrained(
            pretrained_model_name_or_path,
            *model_args,
            config=config,
            cache_dir=cache_dir,
            ignore_mismatched_sizes=ignore_mismatched_sizes,
            force_download=force_download,
            local_files_only=local_files_only,
            token=token,
            revision=revision,
            use_safetensors=use_safetensors,
            **kwargs,
        )

    def forward(
        self,
        past_values: Union[torch.Tensor, List[torch.Tensor]] = None,
        input_ids: Optional[torch.Tensor] = None,
        inputs: Union[torch.Tensor, List[torch.Tensor]] = None,
        freq: Optional[List[int]] = None,
        attention_mask: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        past_key_values: Optional[List[torch.FloatTensor]] = None,
        inputs_embeds: Optional[torch.FloatTensor] = None,
        use_cache: Optional[bool] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        **kwargs
    ) -> Union[Tuple, TimesFmOutputForPrediction]:
        if past_values is None and inputs is not None:
            past_values = inputs
        elif past_values is None and input_ids is not None:
            past_values = input_ids
        
        if past_values is None:
            raise ValueError("Must provide either 'past_values', 'inputs', or 'input_ids'")
        
        output_attentions = output_attentions if output_attentions is not None else getattr(self.config, 'output_attentions', False)
        output_hidden_states = output_hidden_states if output_hidden_states is not None else getattr(self.config, 'output_hidden_states', False)
        return_dict = return_dict if return_dict is not None else getattr(self.config, 'use_return_dict', True)
        
        if isinstance(past_values, list):
            batch_size = len(past_values)
            max_length = max(len(ts) for ts in past_values)
            
            patch_length = self.config.patch_length
            if max_length % patch_length != 0:
                adjusted_length = ((max_length // patch_length) + 1) * patch_length
            else:
                adjusted_length = max_length
            
            padded_sequences = []
            padding_masks = []
            
            for ts in past_values:
                if len(ts) < adjusted_length:
                    pad_length = adjusted_length - len(ts)
                    padded_ts = torch.cat([
                        torch.full((pad_length,), self.config.pad_val, dtype=ts.dtype, device=ts.device),
                        ts
                    ])
                    mask = torch.cat([
                        torch.ones(pad_length, dtype=torch.long, device=ts.device),
                        torch.zeros(len(ts), dtype=torch.long, device=ts.device)
                    ])
                else:
                    if len(ts) > adjusted_length:
                        padded_ts = ts[-adjusted_length:]
                    else:
                        padded_ts = ts
                    mask = torch.zeros(len(padded_ts), dtype=torch.long, device=ts.device)
                
                padded_sequences.append(padded_ts)
                padding_masks.append(mask)
            
            inputs_tensor = torch.stack(padded_sequences)  # (batch_size, seq_len)
            padding_tensor = torch.stack(padding_masks)    # (batch_size, seq_len)
            
        elif isinstance(past_values, torch.Tensor):
            if len(past_values.shape) == 1:
                inputs_tensor = past_values.unsqueeze(0)  # (1, seq_len)
            else:
                inputs_tensor = past_values  # (batch_size, seq_len)
            
            seq_len = inputs_tensor.shape[1]
            patch_length = self.config.patch_length
            
            if seq_len % patch_length != 0:
                pad_length = patch_length - (seq_len % patch_length)
                inputs_tensor = torch.cat([
                    torch.full((inputs_tensor.shape[0], pad_length), self.config.pad_val, 
                            dtype=inputs_tensor.dtype, device=inputs_tensor.device),
                    inputs_tensor
                ], dim=1)
                
                padding_tensor = torch.cat([
                    torch.ones((inputs_tensor.shape[0], pad_length), dtype=torch.long, device=inputs_tensor.device),
                    torch.zeros((inputs_tensor.shape[0], seq_len), dtype=torch.long, device=inputs_tensor.device)
                ], dim=1)
            else:
                padding_tensor = torch.zeros_like(inputs_tensor, dtype=torch.long)
                
        else:
            raise ValueError(f"Unsupported input type: {type(past_values)}")
        
        if freq is None:
            freq_tensor = torch.zeros(inputs_tensor.shape[0], dtype=torch.long, device=inputs_tensor.device)
        else:
            if len(freq) != inputs_tensor.shape[0]:
                if len(freq) == 1:
                    freq = freq * inputs_tensor.shape[0]
                else:
                    freq = freq[:inputs_tensor.shape[0]]
            freq_tensor = torch.tensor(freq, dtype=torch.long, device=inputs_tensor.device)
        
        decoder_outputs = self.decoder(
            past_values=inputs_tensor,
            past_values_padding=padding_tensor,
            freq=freq_tensor
        )
        
        last_hidden_state = decoder_outputs.last_hidden_state
        
        last_timestep_hidden = last_hidden_state[:, -1, :]  # (batch_size, hidden_size)
        
        predictions = self.horizon_ff_layer(last_timestep_hidden)  # (batch_size, horizon_length * num_quantiles)
        
        batch_size = predictions.shape[0]
        num_quantiles = len(self.config.quantiles) if hasattr(self.config, 'quantiles') else 1
        horizon_length = self.config.horizon_length
        
        expected_output_size = horizon_length * (1 + num_quantiles)
        if predictions.shape[1] == expected_output_size:
            reshaped_predictions = predictions.view(batch_size, horizon_length, 1 + num_quantiles)
            mean_predictions = reshaped_predictions[:, :, 0]  # (batch_size, horizon_length)
            full_predictions = reshaped_predictions  # (batch_size, horizon_length, 1 + num_quantiles)
        elif predictions.shape[1] == horizon_length:
            mean_predictions = predictions  # (batch_size, horizon_length)
            full_predictions = predictions.unsqueeze(-1)  # (batch_size, horizon_length, 1)
        else:
            if predictions.shape[1] >= horizon_length:
                mean_predictions = predictions[:, :horizon_length]
            else:
                mean_predictions = predictions
            full_predictions = mean_predictions.unsqueeze(-1)
        
        if not return_dict:
            output = (mean_predictions, full_predictions, last_hidden_state)
            return output
        
        return TimesFmOutputForPrediction(
            mean_predictions=mean_predictions,
            full_predictions=full_predictions,
            last_hidden_state=last_hidden_state,
        )
    
    def _load_pretrained_model_weights(self, *args, **kwargs):
        try:
            return super()._load_pretrained_model_weights(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Standard weight loading failed: {e}, trying custom loading...")
            return self._load_timesfm_weights(*args, **kwargs)

    def _load_timesfm_weights(self, model_file, *args, **kwargs):

        
        if model_file.endswith('.safetensors'):
            state_dict = safetensors.torch.load_file(model_file)
        else:
            state_dict = torch.load(model_file, map_location='cpu')
        
        weight_mapping = {
            "decoder.input_ff_layer": "decoder.input_ff_layer",
            "decoder.freq_emb": "decoder.freq_emb", 
            "decoder.layers": "decoder.layers",
            "horizon_ff_layer": "horizon_ff_layer",
        }
        
        mapped_state_dict = {}
        for key, value in state_dict.items():
            mapped_key = self._map_weight_key(key, weight_mapping)
            if mapped_key:
                mapped_state_dict[mapped_key] = value
        
        missing_keys, unexpected_keys = self.load_state_dict(mapped_state_dict, strict=False)
        
        if missing_keys:
            logger.warning(f"Missing keys when loading TimesFM weights: {missing_keys}")
        if unexpected_keys:
            logger.warning(f"Unexpected keys when loading TimesFM weights: {unexpected_keys}")
        
        return missing_keys, unexpected_keys

    def _map_weight_key(self, key: str, mapping: dict) -> str:
        for pattern, replacement in mapping.items():
            if pattern in key:
                return key.replace(pattern, replacement)
        return key
