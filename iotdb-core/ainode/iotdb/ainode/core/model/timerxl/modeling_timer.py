1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1from typing import List, Optional, Tuple, Union
1
1import torch
1import torch.nn.functional as F
1from torch import nn
1from transformers import Cache, DynamicCache, PreTrainedModel
1from transformers.activations import ACT2FN
1from transformers.modeling_attn_mask_utils import _prepare_4d_causal_attention_mask
1from transformers.modeling_outputs import (
1    MoeCausalLMOutputWithPast,
1    MoeModelOutputWithPast,
1)
1
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.timerxl.configuration_timer import TimerConfig
1from iotdb.ainode.core.model.timerxl.ts_generation_mixin import TSGenerationMixin
1
1logger = Logger()
1
1
1def rotate_half(x):
1    x1 = x[..., : x.shape[-1] // 2]
1    x2 = x[..., x.shape[-1] // 2 :]
1    return torch.cat((-x2, x1), dim=-1)
1
1
1def apply_rotary_pos_emb(q, k, cos, sin, position_ids, unsqueeze_dim=1):
1    cos = cos[position_ids].unsqueeze(unsqueeze_dim)
1    sin = sin[position_ids].unsqueeze(unsqueeze_dim)
1    q_embed = (q * cos) + (rotate_half(q) * sin)
1    k_embed = (k * cos) + (rotate_half(k) * sin)
1    return q_embed, k_embed
1
1
1class TimerPatchEmbedding(nn.Module):
1    def __init__(self, config: TimerConfig):
1        super().__init__()
1        self.input_token_len = config.input_token_len
1        self.emb = nn.Linear(config.input_token_len, config.hidden_size, bias=False)
1
1    def forward(self, hidden_state: torch.Tensor):
1        hidden_state = hidden_state.unfold(
1            dimension=-1, size=self.input_token_len, step=self.input_token_len
1        )
1        return self.emb(hidden_state)
1
1
1class TimerPointEmbedding(nn.Module):
1    def __init__(self, config: TimerConfig):
1        super().__init__()
1        self.emb_layer = nn.Linear(
1            config.input_token_len, config.hidden_size, bias=False
1        )
1        self.gate_layer = nn.Linear(
1            config.input_token_len, config.hidden_size, bias=False
1        )
1        self.act_fn = ACT2FN[config.hidden_act]
1
1    def forward(self, x):
1        emb = self.act_fn(self.gate_layer(x)) * self.emb_layer(x)
1        return emb
1
1
1class TimeMoeRotaryEmbedding(torch.nn.Module):
1    def __init__(self, dim, max_position_embeddings=10000, base=10000, device=None):
1        super().__init__()
1        self.dim = dim
1        self.max_position_embeddings = max_position_embeddings
1        self.base = base
1        inv_freq = 1.0 / (
1            self.base
1            ** (
1                torch.arange(0, self.dim, 2, dtype=torch.int64).float().to(device)
1                / self.dim
1            )
1        )
1        self.register_buffer("inv_freq", inv_freq, persistent=False)
1
1        # Build here to make `torch.jit.trace` work.
1        self._set_cos_sin_cache(
1            seq_len=max_position_embeddings,
1            device=self.inv_freq.device,
1            dtype=torch.get_default_dtype(),
1        )
1
1    def _set_cos_sin_cache(self, seq_len, device, dtype):
1        self.max_seq_len_cached = seq_len
1        t = torch.arange(
1            self.max_seq_len_cached, device=device, dtype=torch.int64
1        ).type_as(self.inv_freq)
1
1        freqs = torch.outer(t, self.inv_freq)
1        # Different from paper, but it uses a different permutation in order to obtain the same calculation
1        emb = torch.cat((freqs, freqs), dim=-1)
1        self.register_buffer("cos_cached", emb.cos().to(dtype), persistent=False)
1        self.register_buffer("sin_cached", emb.sin().to(dtype), persistent=False)
1
1    def forward(self, x, seq_len=None):
1        # x: [bs, num_attention_heads, seq_len, head_size]
1        if seq_len > self.max_seq_len_cached:
1            self._set_cos_sin_cache(seq_len=seq_len, device=x.device, dtype=x.dtype)
1
1        return (
1            self.cos_cached[:seq_len].to(dtype=x.dtype),
1            self.sin_cached[:seq_len].to(dtype=x.dtype),
1        )
1
1
1class TimerAttention(nn.Module):
1    def __init__(self, config: TimerConfig, layer_idx: Optional[int] = None):
1        super().__init__()
1        self.layer_idx = layer_idx
1        self.hidden_size = config.hidden_size
1        self.num_heads = config.num_attention_heads
1        self.head_dim = self.hidden_size // self.num_heads
1        self.attention_dropout = config.attention_dropout
1        self.q_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=True)
1        self.k_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=True)
1        self.v_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=True)
1        self.o_proj = nn.Linear(self.hidden_size, self.hidden_size, bias=False)
1        self.rotary_emb = TimeMoeRotaryEmbedding(
1            self.head_dim, max_position_embeddings=config.max_position_embeddings
1        )
1
1    def forward(
1        self,
1        hidden_states: torch.Tensor,
1        attention_mask: Optional[torch.Tensor] = None,
1        position_ids: Optional[torch.LongTensor] = None,
1        past_key_value: Optional[Cache] = None,
1        output_attentions: bool = False,
1        **kwargs,
1    ) -> Tuple[torch.Tensor, Optional[torch.Tensor], Optional[Cache]]:
1        bsz, q_len, _ = hidden_states.size()
1
1        query_states = self.q_proj(hidden_states)
1        key_states = self.k_proj(hidden_states)
1        value_states = self.v_proj(hidden_states)
1
1        query_states = query_states.view(
1            bsz, q_len, self.num_heads, self.head_dim
1        ).transpose(1, 2)
1        key_states = key_states.view(
1            bsz, q_len, self.num_heads, self.head_dim
1        ).transpose(1, 2)
1        value_states = value_states.view(
1            bsz, q_len, self.num_heads, self.head_dim
1        ).transpose(1, 2)
1
1        kv_seq_len = key_states.shape[-2]
1        if past_key_value is not None:
1            kv_seq_len += past_key_value.get_seq_length(self.layer_idx)
1        cos, sin = self.rotary_emb(value_states, seq_len=kv_seq_len)
1        query_states, key_states = apply_rotary_pos_emb(
1            query_states, key_states, cos, sin, position_ids
1        )
1
1        if past_key_value is not None:
1            key_states, value_states = past_key_value.update(
1                key_states, value_states, self.layer_idx
1            )
1
1        attn_output = F.scaled_dot_product_attention(
1            query_states,
1            key_states,
1            value_states,
1            attention_mask,
1            dropout_p=self.attention_dropout,
1        )
1
1        attn_output = attn_output.transpose(1, 2).contiguous()
1        attn_output = attn_output.reshape(bsz, q_len, self.hidden_size)
1        attn_output = self.o_proj(attn_output)
1
1        if not output_attentions:
1            attn_weights = None
1
1        return attn_output, attn_weights, past_key_value
1
1
1class TimerMLP(nn.Module):
1    def __init__(self, hidden_size: int, intermediate_size: int, hidden_act: str):
1        super().__init__()
1        self.hidden_size = hidden_size
1        self.intermediate_size = intermediate_size
1        self.gate_proj = nn.Linear(self.hidden_size, self.intermediate_size, bias=False)
1        self.up_proj = nn.Linear(self.hidden_size, self.intermediate_size, bias=False)
1        self.down_proj = nn.Linear(self.intermediate_size, self.hidden_size, bias=False)
1        self.act_fn = ACT2FN[hidden_act]
1
1    def forward(self, hidden_state):
1        return self.down_proj(
1            self.act_fn(self.gate_proj(hidden_state)) * self.up_proj(hidden_state)
1        )
1
1
1class TimerDecoderLayer(nn.Module):
1    def __init__(self, config: TimerConfig, layer_idx: int):
1        super().__init__()
1        self.self_attn = TimerAttention(config, layer_idx)
1
1        self.ffn_layer = TimerMLP(
1            hidden_size=config.hidden_size,
1            intermediate_size=config.intermediate_size,
1            hidden_act=config.hidden_act,
1        )
1        self.norm1 = torch.nn.LayerNorm(config.hidden_size)
1        self.norm2 = torch.nn.LayerNorm(config.hidden_size)
1
1    def forward(
1        self,
1        hidden_states: torch.Tensor,
1        attention_mask: Optional[torch.Tensor] = None,
1        position_ids: Optional[torch.LongTensor] = None,
1        past_key_value: Optional[Cache] = None,
1        output_attentions: Optional[bool] = False,
1        **kwargs,
1    ) -> Tuple[
1        torch.FloatTensor,
1        Optional[torch.Tensor],
1        Optional[Cache],
1    ]:
1        residual = hidden_states
1
1        # Self Attention
1        hidden_states, self_attn_weights, present_key_value = self.self_attn(
1            hidden_states=hidden_states,
1            attention_mask=attention_mask,
1            position_ids=position_ids,
1            past_key_value=past_key_value,
1            output_attentions=output_attentions,
1        )
1        hidden_states = residual + hidden_states
1        hidden_states = self.norm1(hidden_states)
1
1        # Fully Connected
1        residual = hidden_states
1        hidden_states = self.ffn_layer(hidden_states)
1        hidden_states = residual + hidden_states
1        hidden_states = self.norm2(hidden_states)
1
1        if not output_attentions:
1            self_attn_weights = None
1
1        return hidden_states, self_attn_weights, present_key_value
1
1
1class TimerPreTrainedModel(PreTrainedModel):
1    config_class = TimerConfig
1    base_model_prefix = "model"
1    supports_gradient_checkpointing = True
1    _no_split_modules = ["TimeDecoderLayer"]
1    _skip_keys_device_placement = "past_key_values"
1    _supports_flash_attn_2 = True
1    _supports_sdpa = False
1    _supports_cache_class = True
1
1    def _init_weights(self, module):
1        std = self.config.initializer_range
1        if isinstance(module, torch.nn.Linear):
1            module.weight.data.normal_(mean=0.0, std=std)
1            if module.bias is not None:
1                module.bias.data.zero_()
1        elif isinstance(module, torch.nn.Embedding):
1            module.weight.data.normal_(mean=0.0, std=std)
1            if module.padding_idx is not None:
1                module.weight.data[module.padding_idx].zero_()
1
1
1class TimerModel(TimerPreTrainedModel):
1    def __init__(self, config: TimerConfig):
1        super().__init__(config)
1        self.embed_layer = TimerPatchEmbedding(config)
1        self.layers = nn.ModuleList(
1            [
1                TimerDecoderLayer(config, layer_idx)
1                for layer_idx in range(config.num_hidden_layers)
1            ]
1        )
1        self.norm = torch.nn.LayerNorm(config.hidden_size)
1        self.gradient_checkpointing = False
1
1    def forward(
1        self,
1        input_ids: torch.FloatTensor = None,
1        attention_mask: Optional[torch.Tensor] = None,
1        position_ids: Optional[torch.LongTensor] = None,
1        past_key_values: Optional[
1            Union[Cache, tuple[tuple[torch.Tensor, torch.Tensor]]]
1        ] = None,
1        inputs_embeds: Optional[torch.FloatTensor] = None,
1        output_attentions: Optional[bool] = None,
1        output_hidden_states: Optional[bool] = None,
1        return_dict: Optional[bool] = None,
1    ) -> Union[Tuple, MoeModelOutputWithPast]:
1        # input_ids is the input of time series, its shape is [batch_size, seq_len]
1        output_attentions = (
1            output_attentions
1            if output_attentions is not None
1            else self.config.output_attentions
1        )
1        output_hidden_states = (
1            output_hidden_states
1            if output_hidden_states is not None
1            else self.config.output_hidden_states
1        )
1
1        return_dict = (
1            return_dict if return_dict is not None else self.config.use_return_dict
1        )
1
1        # retrieve input_ids and inputs_embeds
1        if input_ids is not None and inputs_embeds is not None:
1            raise ValueError(
1                "You cannot specify both decoder_input_ids and decoder_inputs_embeds at the same time"
1            )
1        elif input_ids is not None:
1            batch_size, seq_length = input_ids.shape
1        elif inputs_embeds is not None:
1            batch_size, seq_length, _ = inputs_embeds.shape
1        else:
1            raise ValueError(
1                "You have to specify either decoder_input_ids or decoder_inputs_embeds"
1            )
1
1        if inputs_embeds is None:
1            inputs_embeds = self.embed_layer(input_ids)
1            seq_length = inputs_embeds.shape[1]
1
1        past_key_values_length = 0
1        use_legacy_cache = False
1
1        if past_key_values is not None:
1            use_legacy_cache = not isinstance(past_key_values, Cache)
1            # Converts the legacy cache which is tuple into an equivalent Cache. Used for backward compatibility.
1            if use_legacy_cache:
1                past_key_values = DynamicCache.from_legacy_cache(past_key_values)
1            past_key_values_length = past_key_values.get_seq_length()
1
1        # When training + checkpoints, caching is usually disabled (just do not transfer)
1        if (
1            self.gradient_checkpointing
1            and self.training
1            and isinstance(past_key_values, Cache)
1        ):
1            past_key_values = None
1            past_key_values_length = 0
1
1        if position_ids is None:
1            device = input_ids.device if input_ids is not None else inputs_embeds.device
1            position_ids = torch.arange(
1                past_key_values_length,
1                seq_length + past_key_values_length,
1                dtype=torch.long,
1                device=device,
1            )
1            # position_ids = position_ids.unsqueeze(0).view(-1, seq_length)
1            position_ids = position_ids.view(-1, seq_length)
1        else:
1            position_ids = position_ids.view(-1, seq_length).long()
1
1        # 4d mask is passed through the layers
1        attention_mask = _prepare_4d_causal_attention_mask(
1            attention_mask,
1            (batch_size, seq_length),
1            inputs_embeds,
1            past_key_values_length,
1            sliding_window=None,
1        )
1
1        hidden_states = inputs_embeds
1
1        # decoder layers
1        all_hidden_states = () if output_hidden_states else None
1        all_self_attns = () if output_attentions else None
1        next_decoder_cache = None
1
1        for decoder_layer in self.layers:
1            if output_hidden_states:
1                all_hidden_states += (hidden_states,)
1
1            if self.gradient_checkpointing and self.training:
1                layer_outputs = self._gradient_checkpointing_func(
1                    decoder_layer.__call__,
1                    hidden_states,
1                    attention_mask,
1                    position_ids,
1                    past_key_values,
1                    output_attentions,
1                )
1            else:
1                layer_outputs = decoder_layer(
1                    hidden_states,
1                    attention_mask=attention_mask,
1                    position_ids=position_ids,
1                    past_key_value=past_key_values,
1                    output_attentions=output_attentions,
1                )
1
1            hidden_states = layer_outputs[0]
1
1            if output_attentions:
1                all_self_attns += (layer_outputs[1],)
1
1            if isinstance(past_key_values, Cache):
1                next_decoder_cache = layer_outputs[2]
1
1        hidden_states = self.norm(hidden_states)
1        # add hidden states from the last decoder layer
1        if output_hidden_states:
1            all_hidden_states += (hidden_states,)
1
1        next_cache = None
1        if isinstance(past_key_values, Cache):
1            next_cache = (
1                next_decoder_cache.to_legacy_cache()
1                if use_legacy_cache
1                else next_decoder_cache
1            )
1
1        if not return_dict:
1            return tuple(
1                v
1                for v in [hidden_states, next_cache, all_hidden_states, all_self_attns]
1                if v is not None
1            )
1        return MoeModelOutputWithPast(
1            last_hidden_state=hidden_states,
1            past_key_values=next_cache,
1            hidden_states=all_hidden_states,
1            attentions=all_self_attns,
1        )
1
1
1class TimerForPrediction(TimerPreTrainedModel, TSGenerationMixin):
1    def __init__(self, config: TimerConfig):
1        super().__init__(config)
1        self.config = config
1        self.model = TimerModel(self.config)
1        lm_head_list = []
1        self.output_token_len_map = {}
1        for i, output_token_len in enumerate(self.config.output_token_lens):
1            lm_head_list.append(
1                nn.Linear(self.config.hidden_size, output_token_len, bias=False)
1            )
1            self.output_token_len_map[output_token_len] = i
1        self.lm_heads = nn.ModuleList(lm_head_list)
1        self.loss_function = torch.nn.MSELoss(reduction="none")
1        self.post_init()
1
1    def set_decoder(self, decoder):
1        self.model = decoder
1
1    def get_decoder(self):
1        return self.model
1
1    def forward(
1        self,
1        input_ids: torch.FloatTensor = None,
1        attention_mask: Optional[torch.Tensor] = None,
1        position_ids: Optional[torch.LongTensor] = None,
1        past_key_values: Optional[
1            Union[Cache, tuple[tuple[torch.Tensor, torch.Tensor]]]
1        ] = None,
1        inputs_embeds: Optional[torch.FloatTensor] = None,
1        labels: Optional[torch.FloatTensor] = None,
1        loss_masks: Optional[torch.FloatTensor] = None,
1        output_attentions: Optional[bool] = None,
1        output_hidden_states: Optional[bool] = None,
1        return_dict: Optional[bool] = None,
1        max_output_length: Optional[int] = None,
1        revin: Optional[bool] = False,
1    ) -> Union[Tuple, MoeCausalLMOutputWithPast]:
1
1        output_attentions = (
1            output_attentions
1            if output_attentions is not None
1            else self.config.output_attentions
1        )
1        output_hidden_states = (
1            output_hidden_states
1            if output_hidden_states is not None
1            else self.config.output_hidden_states
1        )
1        return_dict = (
1            return_dict if return_dict is not None else self.config.use_return_dict
1        )
1
1        if revin:
1            mean, std = input_ids.mean(dim=-1, keepdim=True), input_ids.std(
1                dim=-1, keepdim=True
1            )
1            input_ids = (input_ids - mean) / std
1        outputs = self.model(
1            input_ids=input_ids,
1            attention_mask=attention_mask,
1            position_ids=position_ids,
1            past_key_values=past_key_values,
1            inputs_embeds=inputs_embeds,
1            output_attentions=output_attentions,
1            output_hidden_states=output_hidden_states,
1            return_dict=return_dict,
1        )
1
1        hidden_states = outputs[0] if not return_dict else outputs.last_hidden_state
1        predictions = None
1
1        loss = None
1        if labels is not None:
1            ar_loss = 0.0
1            for lm_head, output_token_len in zip(
1                self.lm_heads, self.config.output_token_lens
1            ):
1                one_predictions = lm_head(hidden_states)
1                one_loss = self.calc_ar_loss(
1                    one_predictions, labels, loss_masks, output_token_len
1                )
1                ar_loss += one_loss
1                if predictions is None:
1                    predictions = one_predictions
1            loss = ar_loss / len(self.config.output_token_lens)
1        else:
1            if max_output_length is None:
1                output_token_len = self.config.output_token_lens[0]
1                max_output_length = output_token_len
1            else:
1                output_token_len = self.config.output_token_lens[0]
1                for h in self.config.output_token_lens[1:]:
1                    if h > max_output_length:
1                        break
1                    else:
1                        output_token_len = h
1            lm_head = self.lm_heads[self.output_token_len_map[output_token_len]]
1            predictions = lm_head(hidden_states)[:, -1, :]
1            if output_token_len > max_output_length:
1                predictions = predictions[:, :max_output_length]
1            if revin:
1                predictions = predictions * std + mean
1        if not return_dict:
1            output = (predictions,) + outputs[1:]
1            return (loss,) + output if loss is not None else output
1
1        return MoeCausalLMOutputWithPast(
1            loss=loss,
1            logits=predictions,
1            past_key_values=outputs.past_key_values,
1            hidden_states=outputs.hidden_states,
1            attentions=outputs.attentions,
1        )
1
1    def calc_ar_loss(self, predictions, labels, loss_masks, output_token_len):
1        seq_len = predictions.shape[1] * self.config.input_token_len
1        labels = labels[:, : seq_len - self.config.input_token_len + output_token_len]
1        shift_labels = labels.unfold(
1            dimension=-1, size=output_token_len, step=self.config.input_token_len
1        )
1
1        # Calculate loss with mask
1        losses = self.loss_function(predictions, shift_labels).mean(dim=-1)
1        if loss_masks is not None:
1            losses = losses * loss_masks
1            loss = losses.sum() / loss_masks.sum()
1        else:
1            loss = torch.mean(losses)
1
1        return loss
1
1    def prepare_inputs_for_generation(
1        self,
1        input_ids,
1        past_key_values=None,
1        attention_mask=None,
1        inputs_embeds=None,
1        revin=True,
1        **kwargs,
1    ):
1        # Omit tokens covered by past_key_values
1        if past_key_values is not None:
1            if isinstance(past_key_values, Cache):
1                past_length = past_key_values.get_seq_length()
1            else:
1                past_length = past_key_values[0][0].shape[2]
1
1            # Keep only the unprocessed tokens:
1            # 1 - If the length of the attention_mask exceeds the length of input_ids, then we are in a setting where
1            # some of the inputs are exclusively passed as part of the cache (e.g. when passing input_embeds as
1            # input)
1            if attention_mask is not None and attention_mask.shape[1] > (
1                input_ids.shape[1] // self.config.input_token_len
1            ):
1                input_ids = input_ids[:, -(attention_mask.shape[1] - past_length) :]
1            # 2 - If the past_length is smaller than input_ids', then input_ids holds all input tokens. We can discard
1            # input_ids based on the past_length.
1            elif past_length < (input_ids.shape[1] // self.config.input_token_len):
1                input_ids = input_ids[:, past_length * self.config.input_token_len :]
1            # 3 - Otherwise (past_length >= (input_ids.shape[1] // self.config.input_token_len)), let's assume input_ids only has unprocessed tokens.
1
1        position_ids = kwargs.get("position_ids", None)
1        if attention_mask is not None and position_ids is None:
1            # create position_ids on the fly for batch generation
1            position_ids = attention_mask.long().cumsum(-1) - 1
1            position_ids.masked_fill_(attention_mask == 0, 1)
1            if past_key_values:
1                position_ids = position_ids[
1                    :, -(input_ids.shape[1] // self.config.input_token_len) :
1                ]
1
1        # if `inputs_embeds` are passed, we only want to use them in the 1st generation step
1        if inputs_embeds is not None and past_key_values is None:
1            model_inputs = {"inputs_embeds": inputs_embeds}
1        else:
1            model_inputs = {"input_ids": input_ids}
1
1        model_inputs.update(
1            {
1                "position_ids": position_ids,
1                "past_key_values": past_key_values,
1                "attention_mask": attention_mask,
1                "revin": revin,
1            }
1        )
1        return model_inputs
1