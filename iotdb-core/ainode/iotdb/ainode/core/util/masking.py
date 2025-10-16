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
1import torch
1
1
1class TriangularCausalMask:
1    def __init__(self, B, L, device="cpu"):
1        mask_shape = [B, 1, L, L]
1        with torch.no_grad():
1            self._mask = torch.triu(
1                torch.ones(mask_shape, dtype=torch.bool), diagonal=1
1            ).to(device)
1
1    @property
1    def mask(self):
1        return self._mask
1
1
1class TimerMultivariateMask:
1    def __init__(self, B, n_vars, n_tokens, device="cpu"):
1        mask_shape = [B, 1, n_tokens, n_tokens]
1        with torch.no_grad():
1            self._mask1 = torch.ones((n_vars, n_vars), dtype=torch.bool).to(device)
1            self._mask2 = torch.triu(
1                torch.ones(mask_shape, dtype=torch.bool), diagonal=1
1            ).to(device)
1            self._mask = torch.kron(self._mask1, self._mask2)
1
1    @property
1    def mask(self):
1        return self._mask
1
1
1class TimerCovariateMask:
1    def __init__(self, B, n_vars, n_tokens, device="cpu"):
1        mask_shape = [B, 1, n_tokens, n_tokens]
1        with torch.no_grad():
1            self._mask1 = torch.eye(n_vars, dtype=torch.bool).to(device)
1            self._mask2 = torch.tril(torch.ones(mask_shape, dtype=torch.bool)).to(
1                device
1            )
1            self._mask = ~torch.kron(self._mask1, self._mask2)
1            self._mask[:, :, -n_tokens:, :-n_tokens] = False
1
1    @property
1    def mask(self):
1        return self._mask
1
1
1def prepare_4d_causal_attention_mask(
1    attention_mask,
1    input_shape,  # (B, T_query)
1    inputs_embeds: torch.Tensor,
1    past_key_values_length: int = 0,
1):
1    B, T = input_shape
1    S = T + past_key_values_length
1    dtype, device = inputs_embeds.dtype, inputs_embeds.device
1
1    # 1) causal mask
1    q_pos = torch.arange(
1        past_key_values_length, past_key_values_length + T, device=device
1    )  # [T]
1    k_pos = torch.arange(S, device=device)  # [S]
1    causal = k_pos.unsqueeze(0) <= q_pos.unsqueeze(1)  # [T,S] bool
1
1    mask = torch.zeros((T, S), dtype=dtype, device=device)
1    mask.masked_fill_(~causal, torch.finfo(dtype).min)  # unvisible → -inf
1    mask = mask.unsqueeze(0).unsqueeze(0)  # [1,1,T,S]
1
1    # 2) padding mask
1    if attention_mask is not None:
1        pad = (1.0 - attention_mask.to(dtype)) * torch.finfo(dtype).min  # [B,S]
1        pad = pad[:, None, None, :]  # [B,1,1,S]
1    else:
1        pad = 0.0
1
1    return mask + pad  # [B,1,T,S]
1