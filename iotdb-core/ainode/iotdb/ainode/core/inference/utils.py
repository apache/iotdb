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
1import secrets
1import string
1
1import torch
1from transformers.modeling_outputs import MoeCausalLMOutputWithPast
1
1
1def generate_req_id(length=10, charset=string.ascii_letters + string.digits) -> str:
1    """
1    Generate a random req_id string of specified length.
1    The length is 10 by default, with 10^{17} possible combinations.
1    """
1    return "".join(secrets.choice(charset) for _ in range(length))
1
1
1def _slice_tensor(t, s, e):
1    return None if t is None else t[s:e]
1
1
1def _slice_tuple_of_tensors(tup, s, e):
1    """
1    hidden_states / attentions: Tuple[layer0, layer1, ...]
1    every layer maybe Tensor or None。
1    """
1    if tup is None:
1        return None
1    sliced = []
1    for x in tup:
1        sliced.append(_slice_tensor(x, s, e) if torch.is_tensor(x) else x)
1    return tuple(sliced)
1
1
1def _slice_pkv(pkv, s, e):
1    if pkv is None:
1        return None
1    out = []
1    for layer in pkv:  # layer: Tuple[key, value, ...]
1        out.append(tuple(x[s:e] for x in layer))
1    return out
1
1
1def split_moe_output(batch_out: MoeCausalLMOutputWithPast, split_sizes):
1    """
1    split batch_out with type: MoeCausalLMOutputWithPast into len(split_sizes)
1    split_sizes[i] = ith request's batch_size。
1    """
1    outs = []
1    start = 0
1    for bsz in split_sizes:
1        end = start + bsz
1        outs.append(
1            MoeCausalLMOutputWithPast(
1                loss=_slice_tensor(batch_out.loss, start, end),
1                logits=batch_out.logits[start:end],
1                past_key_values=_slice_pkv(batch_out.past_key_values, start, end),
1                hidden_states=_slice_tuple_of_tensors(
1                    batch_out.hidden_states, start, end
1                ),
1                attentions=_slice_tuple_of_tensors(batch_out.attentions, start, end),
1            )
1        )
1        start = end
1    return outs
1