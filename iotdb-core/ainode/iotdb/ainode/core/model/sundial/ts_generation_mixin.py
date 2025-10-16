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
1import warnings
1from typing import Any, Callable, Dict, List, Optional, Union
1
1import torch
1from transformers import GenerationMixin, LogitsProcessorList, StoppingCriteriaList
1from transformers.generation import EosTokenCriteria, validate_stopping_criteria
1from transformers.generation.utils import (
1    GenerateDecoderOnlyOutput,
1    GenerateEncoderDecoderOutput,
1    GenerateNonBeamOutput,
1    GenerateOutput,
1    GenerationConfig,
1)
1from transformers.utils import ModelOutput
1
1
1class TSGenerationMixin(GenerationMixin):
1    @torch.no_grad()
1    def generate(
1        self,
1        inputs: Optional[torch.Tensor] = None,
1        generation_config: Optional[GenerationConfig] = None,
1        logits_processor: Optional[LogitsProcessorList] = None,
1        stopping_criteria: Optional[StoppingCriteriaList] = None,
1        prefix_allowed_tokens_fn: Optional[
1            Callable[[int, torch.Tensor], List[int]]
1        ] = None,
1        synced_gpus: Optional[bool] = None,
1        assistant_model: Optional["PreTrainedModel"] = None,
1        streamer: Optional["BaseStreamer"] = None,
1        negative_prompt_ids: Optional[torch.Tensor] = None,
1        negative_prompt_attention_mask: Optional[torch.Tensor] = None,
1        revin: Optional[bool] = True,
1        num_samples: Optional[int] = 1,
1        **kwargs,
1    ) -> Union[GenerateOutput, torch.LongTensor]:
1        if len(inputs.shape) != 2:
1            raise ValueError("Input shape must be: [batch_size, seq_len]")
1        batch_size, cur_len = inputs.shape
1        if cur_len < self.config.input_token_len:
1            raise ValueError(
1                f"Input length must be at least {self.config.input_token_len}"
1            )
1        if revin:
1            means = inputs.mean(dim=-1, keepdim=True)
1            stdev = inputs.std(dim=-1, keepdim=True, unbiased=False) + 1e-5
1            inputs = (inputs - means) / stdev
1        outputs = super().generate(
1            inputs=inputs,
1            generation_config=generation_config,
1            logits_processor=logits_processor,
1            stopping_criteria=stopping_criteria,
1            prefix_allowed_tokens_fn=prefix_allowed_tokens_fn,
1            synced_gpus=synced_gpus,
1            assistant_model=assistant_model,
1            streamer=streamer,
1            negative_prompt_ids=negative_prompt_ids,
1            negative_prompt_attention_mask=negative_prompt_attention_mask,
1            num_samples=num_samples,
1            **kwargs,
1        )
1        if revin:
1            stdev = stdev.unsqueeze(1).repeat(1, num_samples, 1)
1            means = means.unsqueeze(1).repeat(1, num_samples, 1)
1            outputs = (outputs * stdev) + means
1        return outputs
1
1    def _sample(
1        self,
1        input_ids: torch.Tensor,
1        logits_processor: Optional[LogitsProcessorList] = None,
1        stopping_criteria: Optional[StoppingCriteriaList] = None,
1        max_length: Optional[int] = None,
1        pad_token_id: Optional[int] = None,
1        eos_token_id: Optional[Union[int, List[int]]] = None,
1        output_attentions: Optional[bool] = None,
1        output_hidden_states: Optional[bool] = None,
1        output_scores: Optional[bool] = None,
1        output_logits: Optional[bool] = None,
1        return_dict_in_generate: Optional[bool] = None,
1        synced_gpus: bool = False,
1        streamer: Optional["BaseStreamer"] = None,
1        **model_kwargs,
1    ) -> Union[GenerateNonBeamOutput, torch.Tensor]:
1        input_ids = input_ids.to(self.device)
1        batch_size, cur_len = input_ids.shape
1        # init values
1        logits_processor = (
1            logits_processor if logits_processor is not None else LogitsProcessorList()
1        )
1        stopping_criteria = (
1            stopping_criteria
1            if stopping_criteria is not None
1            else StoppingCriteriaList()
1        )
1        if max_length is not None:
1            warnings.warn(
1                "`max_length` is deprecated in this function, use"
1                " `stopping_criteria=StoppingCriteriaList([MaxLengthCriteria(max_length=max_length)])` instead.",
1                UserWarning,
1            )
1            stopping_criteria = validate_stopping_criteria(
1                stopping_criteria, max_length
1            )
1        pad_token_id = (
1            pad_token_id
1            if pad_token_id is not None
1            else self.generation_config.pad_token_id
1        )
1        if eos_token_id is not None:
1            stopping_criteria.append(EosTokenCriteria(eos_token_id=eos_token_id))
1        else:
1            # remove when the method is totally private
1            # need to get `eos_token_id` and add stopping criteria, so that generation does not go forever
1            eos_token_id = [
1                criteria.eos_token_id.tolist()
1                for criteria in stopping_criteria
1                if hasattr(criteria, "eos_token_id")
1            ]
1            eos_token_id = eos_token_id[0] if eos_token_id else None
1            if eos_token_id is None and self.generation_config.eos_token_id is not None:
1                eos_token_id = self.generation_config.eos_token_id
1                stopping_criteria.append(EosTokenCriteria(eos_token_id=eos_token_id))
1
1        if isinstance(eos_token_id, int):
1            eos_token_id = [eos_token_id]
1        output_scores = (
1            output_scores
1            if output_scores is not None
1            else self.generation_config.output_scores
1        )
1        output_attentions = (
1            output_attentions
1            if output_attentions is not None
1            else self.generation_config.output_attentions
1        )
1        output_hidden_states = (
1            output_hidden_states
1            if output_hidden_states is not None
1            else self.generation_config.output_hidden_states
1        )
1        return_dict_in_generate = (
1            return_dict_in_generate
1            if return_dict_in_generate is not None
1            else self.generation_config.return_dict_in_generate
1        )
1
1        # init attention / hidden states / scores tuples
1        raw_logits = () if (return_dict_in_generate and output_logits) else None
1        scores = () if (return_dict_in_generate and output_scores) else None
1        decoder_attentions = (
1            () if (return_dict_in_generate and output_attentions) else None
1        )
1        cross_attentions = (
1            () if (return_dict_in_generate and output_attentions) else None
1        )
1        decoder_hidden_states = (
1            () if (return_dict_in_generate and output_hidden_states) else None
1        )
1
1        # if model is an encoder-decoder, retrieve encoder attention weights and hidden states
1        if return_dict_in_generate and self.config.is_encoder_decoder:
1            encoder_attentions = (
1                model_kwargs["encoder_outputs"].get("attentions")
1                if output_attentions
1                else None
1            )
1            encoder_hidden_states = (
1                model_kwargs["encoder_outputs"].get("hidden_states")
1                if output_hidden_states
1                else None
1            )
1
1        # keep track of which sequences are already finished
1        if "inputs_embeds" in model_kwargs:
1            cur_len = model_kwargs["inputs_embeds"].shape[1]
1        this_peer_finished = False
1        unfinished_sequences = torch.ones(
1            batch_size, dtype=torch.long, device=input_ids.device
1        )
1        model_kwargs["cache_position"] = torch.arange(cur_len, device=input_ids.device)
1        true_seq_len = (
1            cur_len + self.config.input_token_len - 1
1        ) // self.config.input_token_len
1        model_kwargs["attention_mask"] = model_kwargs["attention_mask"][
1            :, -true_seq_len:
1        ]
1        max_length = stopping_criteria.max_length
1        generate_results = None
1        while self._has_unfinished_sequences(
1            this_peer_finished, synced_gpus, device=input_ids.device
1        ):
1            # prepare model inputs
1            model_inputs = self.prepare_inputs_for_generation(input_ids, **model_kwargs)
1
1            input_length = input_ids.shape[1]
1
1            # forward pass to get next token
1            outputs = self(
1                **model_inputs,
1                return_dict=True,
1                output_attentions=output_attentions,
1                output_hidden_states=output_hidden_states,
1                max_output_length=max_length - input_length,
1            )
1
1            if synced_gpus and this_peer_finished:
1                continue  # don't waste resources running the code we don't need
1            next_token_logits = outputs.logits
1
1            # pre-process distribution
1            next_tokens_scores = logits_processor(input_ids, next_token_logits)
1
1            # Store scores, attentions and hidden_states when required
1            if return_dict_in_generate:
1                if output_scores:
1                    scores += (next_tokens_scores,)
1                if output_logits:
1                    raw_logits += (next_token_logits,)
1                if output_attentions:
1                    decoder_attentions += (
1                        (outputs.decoder_attentions,)
1                        if self.config.is_encoder_decoder
1                        else (outputs.attentions,)
1                    )
1                    if self.config.is_encoder_decoder:
1                        cross_attentions += (outputs.cross_attentions,)
1
1                if output_hidden_states:
1                    decoder_hidden_states += (
1                        (outputs.decoder_hidden_states,)
1                        if self.config.is_encoder_decoder
1                        else (outputs.hidden_states,)
1                    )
1
1            # argmax
1            # next_tokens = torch.argmax(next_tokens_scores, dim=-1)
1            next_tokens = next_tokens_scores
1
1            # finished sentences should have their next token be a padding token
1            if eos_token_id is not None:
1                if pad_token_id is None:
1                    raise ValueError(
1                        "If `eos_token_id` is defined, make sure that `pad_token_id` is defined."
1                    )
1                next_tokens = next_tokens * unfinished_sequences + pad_token_id * (
1                    1 - unfinished_sequences
1                )
1
1            # update generated ids, model inputs, and length for next step
1            horizon_length = next_tokens.shape[-1] // self.config.input_token_len
1
1            past_key_values = model_kwargs.get("past_key_values")
1            if past_key_values is None or generate_results is None:
1                generate_results = next_tokens
1            else:
1                generate_results = torch.cat([generate_results, next_tokens], dim=-1)
1            input_ids = torch.cat([input_ids, next_tokens.median(dim=1)[0]], dim=-1)
1
1            if streamer is not None:
1                streamer.put(next_tokens.cpu())
1            model_kwargs = self._update_model_kwargs_for_generation(
1                outputs,
1                model_kwargs,
1                horizon_length=horizon_length,
1                is_encoder_decoder=self.config.is_encoder_decoder,
1            )
1            unfinished_sequences = unfinished_sequences & ~stopping_criteria(
1                input_ids, scores
1            )
1            this_peer_finished = unfinished_sequences.max() == 0
1
1        if input_ids.shape[-1] > max_length:
1            input_ids = input_ids[:, :max_length]
1
1        if streamer is not None:
1            streamer.end()
1
1        if return_dict_in_generate:
1            if self.config.is_encoder_decoder:
1                return GenerateEncoderDecoderOutput(
1                    sequences=input_ids,
1                    scores=scores,
1                    logits=raw_logits,
1                    encoder_attentions=encoder_attentions,
1                    encoder_hidden_states=encoder_hidden_states,
1                    decoder_attentions=decoder_attentions,
1                    cross_attentions=cross_attentions,
1                    decoder_hidden_states=decoder_hidden_states,
1                    past_key_values=model_kwargs.get("past_key_values"),
1                )
1            else:
1                return GenerateDecoderOnlyOutput(
1                    sequences=input_ids,
1                    scores=scores,
1                    logits=raw_logits,
1                    attentions=decoder_attentions,
1                    hidden_states=decoder_hidden_states,
1                    past_key_values=model_kwargs.get("past_key_values"),
1                )
1        else:
1            return generate_results[:, :, : (max_length - cur_len)]
1
1    def _update_model_kwargs_for_generation(
1        self,
1        outputs: ModelOutput,
1        model_kwargs: Dict[str, Any],
1        horizon_length: int = 1,
1        is_encoder_decoder: bool = False,
1        standardize_cache_format: bool = False,
1    ) -> Dict[str, Any]:
1        # update past_key_values
1        if "past_key_values" in outputs:
1            model_kwargs["past_key_values"] = outputs.past_key_values
1        elif "mems" in outputs:
1            model_kwargs["past_key_values"] = outputs.mems
1        elif "past_buckets_states" in outputs:
1            model_kwargs["past_key_values"] = outputs.past_buckets_states
1
1        if getattr(outputs, "state", None) is not None:
1            model_kwargs["state"] = outputs.state
1
1        # update token_type_ids with last value
1        if "token_type_ids" in model_kwargs:
1            token_type_ids = model_kwargs["token_type_ids"]
1            model_kwargs["token_type_ids"] = torch.cat(
1                [token_type_ids, token_type_ids[:, -1].unsqueeze(-1)], dim=-1
1            )
1
1        if not is_encoder_decoder:
1            # update attention mask
1            if "attention_mask" in model_kwargs:
1                attention_mask = model_kwargs["attention_mask"]
1                model_kwargs["attention_mask"] = torch.cat(
1                    [
1                        attention_mask,
1                        attention_mask.new_ones(
1                            (attention_mask.shape[0], horizon_length)
1                        ),
1                    ],
1                    dim=-1,
1                )
1        else:
1            # update decoder attention mask
1            if "decoder_attention_mask" in model_kwargs:
1                decoder_attention_mask = model_kwargs["decoder_attention_mask"]
1                model_kwargs["decoder_attention_mask"] = torch.cat(
1                    [
1                        decoder_attention_mask,
1                        decoder_attention_mask.new_ones(
1                            (decoder_attention_mask.shape[0], horizon_length)
1                        ),
1                    ],
1                    dim=-1,
1                )
1
1        if (
1            "cache_position" in model_kwargs
1            and model_kwargs["cache_position"] is not None
1        ):
1            model_kwargs["cache_position"] = (
1                model_kwargs["cache_position"][-1:] + horizon_length
1            )
1
1        return model_kwargs
1