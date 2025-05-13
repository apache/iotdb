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

import torch
import os
from torch import nn
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from ainode.TimerXL.layers.Transformer_EncDec import TimerDecoderLayer
from ainode.TimerXL.layers.Embed import TimerPatchEmbedding
from ainode.TimerXL.models.configuration_timer import TimerxlConfig
from ainode.core.util.masking import prepare_4d_causal_attention_mask
from ainode.core.util.huggingface_cache import Cache, DynamicCache

from safetensors.torch import load_file as load_safetensors
from huggingface_hub import hf_hub_download

from ainode.core.log import Logger
logger = Logger()

@dataclass
class Output:
    outputs: torch.Tensor
    past_key_values: Optional[Any] = None

class TimerModel(nn.Module):
    def __init__(self, config: TimerxlConfig):
        super().__init__()
        self.config = config
        self.embed_layer = TimerPatchEmbedding(config)
        self.layers = nn.ModuleList(
            [TimerDecoderLayer(config, layer_idx)
             for layer_idx in range(config.num_hidden_layers)]
        )
        self.norm = torch.nn.LayerNorm(config.hidden_size)
        self.gradient_checkpointing = False

    def forward(
        self,
        input_ids: torch.FloatTensor = None,
        attention_mask: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.LongTensor] = None,
        past_key_values: Optional[List[torch.FloatTensor]] = None,
        use_cache: bool = None,
    ):
        # input_ids is the input of time series, its shape is [batch_size, seq_len]
        
        if input_ids is not None:
            batch_size, seq_length = input_ids.shape
        else:
            raise ValueError(
                "You have to specify either decoder_input_ids or decoder_inputs_embeds")

        inputs_embeds = self.embed_layer(input_ids)    
        
        seq_length = inputs_embeds.shape[1]

        past_key_values_length = 0

        if use_cache:
            use_legacy_cache = not isinstance(past_key_values, Cache)
            if use_legacy_cache:
                past_key_values = DynamicCache.from_legacy_cache(
                    past_key_values)
            past_key_values_length = past_key_values.get_usable_length(
                seq_length)

        if position_ids is None:
            device = input_ids.device if input_ids is not None else inputs_embeds.device
            position_ids = torch.arange(
                past_key_values_length, seq_length + past_key_values_length, dtype=torch.long, device=device
            )
            position_ids = position_ids.view(-1, seq_length)
        else:
            position_ids = position_ids.view(-1, seq_length).long()

        # 4d mask is passed through the layers
        attention_mask = prepare_4d_causal_attention_mask(
            attention_mask,
            (batch_size, seq_length),
            inputs_embeds,
            past_key_values_length,
        )

        hidden_states = inputs_embeds

        # decoder layers
        next_decoder_cache = None

        for decoder_layer in self.layers:
            layer_outputs = decoder_layer(
                hidden_states,
                attention_mask=attention_mask,
                position_ids=position_ids,
                past_key_value=past_key_values,
                use_cache=use_cache,
            )

            hidden_states = layer_outputs[0]
            
            if use_cache:
                next_decoder_cache = layer_outputs[1]
                
        hidden_states = self.norm(hidden_states)

        next_cache = None
        if use_cache:
            next_cache = next_decoder_cache.to_legacy_cache(
            ) if use_legacy_cache else next_decoder_cache

        return Output(
            outputs=hidden_states,
            past_key_values=next_cache
        )

class TimerForPrediction(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.model = TimerModel(self.config)
        lm_head_list = []
        self.output_token_len_map = {}
        for i, output_token_len in enumerate(self.config.output_token_lens):
            lm_head_list.append(
                nn.Linear(self.config.hidden_size, output_token_len, bias=False))
            self.output_token_len_map[output_token_len] = i
        self.lm_heads = nn.ModuleList(lm_head_list)
        self.loss_function = torch.nn.MSELoss(reduction='none')
        
    def forward(
        self,
        input_ids: torch.FloatTensor = None,
        attention_mask: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.LongTensor] = None,
        past_key_values: Optional[List[torch.FloatTensor]] = None,
        use_cache: Optional[bool] = None,
        max_output_length: Optional[int] = None,
        revin: Optional[bool] = True,
    ):
        if revin:
            means, stdev = input_ids.mean(dim=-1, keepdim=True), input_ids.std(dim=-1, keepdim=True)
            input_ids = (input_ids - means) / stdev
   
        outputs = self.model(
            input_ids=input_ids,
            attention_mask=attention_mask,
            position_ids=position_ids,
            past_key_values=past_key_values,
            use_cache=use_cache,
        )
        hidden_states = outputs.outputs
        
        if max_output_length is None:
            output_token_len = self.config.output_token_lens[0]
            max_output_length = output_token_len
        else:
            output_token_len = self.config.output_token_lens[0]
            for h in self.config.output_token_lens[1:]:
                if h > max_output_length:
                    break
                else:
                    output_token_len = h
        
        lm_head = self.lm_heads[self.output_token_len_map[output_token_len]]
        predictions = lm_head(hidden_states)[:, -1, :]
                    
        if output_token_len > max_output_length:
            predictions = predictions[:, :max_output_length]
        if revin:
            predictions = predictions * stdev + means
            
        return Output(predictions, outputs.past_key_values)


class Model(nn.Module):
    """
    Timer-XL: Long-Context Transformers for Unified Time Series Forecasting 

    Paper: https://arxiv.org/abs/2410.04803
    
    GitHub: https://github.com/thuml/Timer-XL
    
    Citation: @article{liu2024timer,
        title={Timer-XL: Long-Context Transformers for Unified Time Series Forecasting},
        author={Liu, Yong and Qin, Guo and Huang, Xiangdong and Wang, Jianmin and Long, Mingsheng},
        journal={arXiv preprint arXiv:2410.04803},
        year={2024}
    }
    """
    def __init__(self, config: TimerxlConfig):
        super().__init__()
        self.config = config      # can't be scripted by torch
        
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = TimerForPrediction(config).to(self.device)
        
        if config.ckpt_path is not None and config.ckpt_path != '':
            if config.ckpt_path.endswith('.pt') or config.ckpt_path.endswith('.pth'):
                state_dict = torch.load(config.ckpt_path)
            elif config.ckpt_path.endswith('.safetensors'):
                if not os.path.exists(config.ckpt_path):
                    logger.info(f"Checkpoint not found at {config.ckpt_path}, downloading from HuggingFace...")
                    repo_id = "thuml/timer-base-84m"
                    try:
                        config.ckpt_path = hf_hub_download(repo_id=repo_id, filename=os.path.basename(config.ckpt_path), local_dir=os.path.dirname(config.ckpt_path))
                        logger.info(f"Got checkpoint to {config.ckpt_path}")
                    except Exception as e:
                        logger.error(f"Failed to download checkpoint to {config.ckpt_path} due to {e}")
                        raise e
                state_dict = load_safetensors(config.ckpt_path)
            else:
                raise ValueError('unsupported model weight type')
            # If there is no key beginning with 'model.model' in state_dict, add a 'model.' before all keys. (The model code here has an additional layer of encapsulation compared to the code on huggingface.)
            if not any(k.startswith('model.model') for k in state_dict.keys()):
                state_dict = {'model.' + k: v for k, v in state_dict.items()}
            self.load_state_dict(state_dict, strict=True)
        
    def set_device(self, device):
        self.model.to(device)
        self.device = next(self.model.parameters()).device
        
    def inference(self, x, max_new_tokens: int = 96):
        # x.shape: [L, C], type: DataFrame
        # here we only except C=1 temporarily
        # change [L, C=1] to [batchsize=1, L]
        self.device = next(self.model.parameters()).device
        
        x = torch.tensor(x, dtype=next(self.model.parameters()).dtype, device=self.device)
        x = x.view(1, -1)

        preds = self.forward(x, max_new_tokens)
        preds = preds.detach().cpu().numpy()

        return preds 

    def forward(self, x, max_new_tokens: int = 96):
        # self.config.is_encoder_decoder = False
        self.eval()
        self.device = next(self.model.parameters()).device
        
        use_cache = self.config.use_cache
        all_input_ids = x
        
        attention_mask = self.prepare_attention_mask_for_generation(all_input_ids)
        all_input_ids_length = all_input_ids.shape[-1]
        max_length = max_new_tokens + all_input_ids_length
        
        all_input_ids = all_input_ids.to(self.device)
        batch_size, cur_len = all_input_ids.shape
        
        unfinished_sequences = torch.ones(batch_size, dtype=torch.long, device=all_input_ids.device)
        cache_position = torch.arange(cur_len, device=all_input_ids.device)
        true_seq_len = cur_len // self.config.input_token_len
        attention_mask = attention_mask[:, -true_seq_len:]
        
        this_peer_finished = False
        past_key_values = None
        position_ids = None
        while not this_peer_finished:
            (   
                input_ids, 
                position_ids, 
                past_key_values, 
                attention_mask, 
                revin
            ) = self.prepare_inputs_for_generation(
                all_input_ids, 
                past_key_values=past_key_values,
                attention_mask=attention_mask,
                # position_ids=position_ids     # Wrong?!
                position_ids=None               # True?!    based on huggingface code
            )

            input_length = all_input_ids.shape[1]
            
            # forward pass to get next token       
            outputs = self.model(
                input_ids,
                attention_mask=attention_mask,
                position_ids=position_ids,
                past_key_values=past_key_values,
                use_cache=use_cache,
                max_output_length=max_length - input_length,
                revin=revin
            )
            
            next_tokens = outputs.outputs
            
            # update generated ids, model inputs, and length for next step
            horizon_length = next_tokens.shape[1] // self.config.input_token_len
            
            all_input_ids = torch.cat([all_input_ids, next_tokens], dim=-1)
            (
                past_key_values,
                attention_mask,
                cache_position
            ) = self._update_model_kwargs_for_generation(
                outputs,
                attention_mask=attention_mask,
                horizon_length=horizon_length,
                cache_position=cache_position,
            )
            
            unfinished_sequences = unfinished_sequences & (all_input_ids.shape[1] < max_length)
            this_peer_finished = unfinished_sequences.max() == 0
            
        if all_input_ids.shape[1] > max_length:
            all_input_ids = all_input_ids[:, :max_length]
        
        return all_input_ids[:, -(max_length - cur_len):]
        
    def prepare_attention_mask_for_generation(
        self,
        inputs: torch.Tensor,
    ) -> torch.LongTensor:
        return torch.ones(inputs.shape[:2], dtype=torch.long, device=inputs.device)
        
    def prepare_inputs_for_generation(
        self, input_ids, past_key_values=None, attention_mask=None, revin=True, position_ids=None
    ):
        # Omit tokens covered by past_key_values
        if past_key_values is not None:
            if isinstance(past_key_values, Cache):
                cache_length = past_key_values.get_seq_length()
                if isinstance(past_key_values, DynamicCache):
                    past_length = past_key_values.seen_tokens
                else:
                    past_length = cache_length

                max_cache_length = past_key_values.get_max_length()
            else:
                cache_length = past_length = past_key_values[0][0].shape[2]
                max_cache_length = None

            # Keep only the unprocessed tokens:
            # 1 - If the length of the attention_mask exceeds the length of input_ids, then we are in a setting where
            # some of the inputs are exclusively passed as part of the cache (e.g. when passing input_embeds as
            # input)
            if attention_mask is not None and attention_mask.shape[1] > (input_ids.shape[1] // self.config.input_token_len):
                input_ids = input_ids[:, -
                                      (attention_mask.shape[1] - past_length):]
            # 2 - If the past_length is smaller than input_ids', then input_ids holds all input tokens. We can discard
            # input_ids based on the past_length.
            elif past_length < (input_ids.shape[1] // self.config.input_token_len):
                input_ids = input_ids[:, past_length *
                                      self.config.input_token_len:]
            # 3 - Otherwise (past_length >= (input_ids.shape[1] // self.config.input_token_len)), let's assume input_ids only has unprocessed tokens.

            # If we are about to go beyond the maximum cache length, we need to crop the input attention mask.
            if (
                    max_cache_length is not None
                    and attention_mask is not None
                    and cache_length + (input_ids.shape[1] // self.config.input_token_len) > max_cache_length
            ):
                attention_mask = attention_mask[:, -max_cache_length:]

        if attention_mask is not None and position_ids is None:
            # create position_ids on the fly for batch generation
            position_ids = attention_mask.long().cumsum(-1) - 1
            position_ids.masked_fill_(attention_mask == 0, 1)
            if past_key_values:
                position_ids = position_ids[:, -
                        (input_ids.shape[1] // self.config.input_token_len):]

        return (input_ids, position_ids, past_key_values, attention_mask, revin)
    
    def _update_model_kwargs_for_generation(
            self,
            outputs,
            attention_mask = None,
            cache_position = None,
            horizon_length: int = 1,
    ) -> Dict[str, Any]:
        # update past_key_values
        past_key_values = outputs.past_key_values

        # update attention mask
        if attention_mask is not None:
            attention_mask = torch.cat(
                [attention_mask, attention_mask.new_ones((attention_mask.shape[0], horizon_length))], dim=-1
            )

        if cache_position is not None:
            cache_position = cache_position[-1:] + horizon_length

        return (past_key_values, attention_mask, cache_position)