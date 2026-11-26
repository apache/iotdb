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
import secrets
import string

import torch


def generate_req_id(length=10, charset=string.ascii_letters + string.digits) -> str:
    """
    Generate a random req_id string of specified length.
    The length is 10 by default, with 10^{17} possible combinations.
    """
    return "".join(secrets.choice(charset) for _ in range(length))


def _slice_tensor(t, s, e):
    return None if t is None else t[s:e]


def _slice_tuple_of_tensors(tup, s, e):
    """
    hidden_states / attentions: Tuple[layer0, layer1, ...]
    every layer maybe Tensor or None。
    """
    if tup is None:
        return None
    sliced = []
    for x in tup:
        sliced.append(_slice_tensor(x, s, e) if torch.is_tensor(x) else x)
    return tuple(sliced)


def _slice_pkv(pkv, s, e):
    if pkv is None:
        return None
    out = []
    for layer in pkv:  # layer: Tuple[key, value, ...]
        out.append(tuple(x[s:e] for x in layer))
    return out


# def split_moe_output(batch_out: MoeCausalLMOutputWithPast, split_sizes):
#     """
#     split batch_out with type: MoeCausalLMOutputWithPast into len(split_sizes)
#     split_sizes[i] = ith request's batch_size。
#     """
#     outs = []
#     start = 0
#     for bsz in split_sizes:
#         end = start + bsz
#         outs.append(
#             MoeCausalLMOutputWithPast(
#                 loss=_slice_tensor(batch_out.loss, start, end),
#                 logits=batch_out.logits[start:end],
#                 past_key_values=_slice_pkv(batch_out.past_key_values, start, end),
#                 hidden_states=_slice_tuple_of_tensors(
#                     batch_out.hidden_states, start, end
#                 ),
#                 attentions=_slice_tuple_of_tensors(batch_out.attentions, start, end),
#             )
#         )
#         start = end
#     return outs
