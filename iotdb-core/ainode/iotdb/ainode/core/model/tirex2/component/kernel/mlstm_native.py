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
# Copyright (c) NXAI GmbH.
# SPDX-License-Identifier: Apache-2.0

"""mLSTM chunkwise forward pass in plain PyTorch, used on CPU/MPS and when gradients are needed.

Vendored from https://github.com/NX-AI/mlstm_kernels (``mlstm_kernels==2.0.6``), reduced to what
TiRex-2 uses: ``mLSTMBackend`` in ``train_with_padding`` mode without initial or last states, with the
``chunkwise--native_autograd`` kernel. The computations and their order are kept as upstream, so the
results are bitwise identical. See ``mlstm_triton.py`` for the CUDA kernel.
Upstream's padding wrapper ignores the backend's chunk_size and eps, so they are always 64 and 1e-6.

Notation: B batch, NH heads, S sequence length, DHQK / DHHV head dims of q, k / v, NC number of chunks,
L chunk size. vecB is the cumulative log forget gate within a chunk, vecA = vecB[-1] - vecB + vecI the
gate contribution from each step to the next chunk state, scaG = vecB[-1] the decay of a whole chunk.
"""

import torch
import torch.nn.functional as F


def pad_to_chunks(q, k, v, i, f, chunk_size):
    """Pad S to a multiple of the chunk size with steps that keep the state: input gate 0, forget gate 1."""
    S = q.shape[2]
    pad = ((S - 1) // chunk_size + 1) * chunk_size - S
    q, k, v = (F.pad(x, (0, 0, 0, pad), value=0.0) for x in (q, k, v))
    return q, k, v, F.pad(i, (0, pad), value=-25.0), F.pad(f, (0, pad), value=25.0)


def mlstm_chunkwise_native(
    q: torch.Tensor,  # (B, NH, S, DHQK)
    k: torch.Tensor,  # (B, NH, S, DHQK)
    v: torch.Tensor,  # (B, NH, S, DHHV)
    i: torch.Tensor,  # (B, NH, S) input gate preactivation
    f: torch.Tensor,  # (B, NH, S) forget gate preactivation
    chunk_size: int = 64,
    eps: float = 1e-6,
) -> torch.Tensor:  # (B, NH, S, DHHV)
    """mLSTM over a full sequence from zero states, in plain PyTorch (any device, differentiable)."""
    B, NH, S, DHQK = q.shape
    DHHV = v.shape[-1]
    q, k, v, i, f = pad_to_chunks(q, k, v, i, f, chunk_size)
    NC, L = q.shape[2] // chunk_size, chunk_size
    qk_scale = DHQK**-0.5

    vecI = i.reshape(B, NH, NC, L)
    vecB = F.logsigmoid(f.reshape(B, NH, NC, L)).cumsum(-1)
    matQ = q.reshape(B, NH, NC, L, DHQK)
    matK = k.reshape(B, NH, NC, L, DHQK)
    matV = v.reshape(B, NH, NC, L, DHHV)

    # Recurrently compute the states (C, n, m) entering each chunk, stored in the dtype of k.
    vecA = vecB[..., -1, None] - vecB + vecI
    scaG = vecB[..., -1]
    scaA_max = vecA.max(-1).values
    matC_k = torch.zeros((B, NH, DHQK, DHHV), dtype=k.dtype, device=k.device)
    vecN_k = torch.zeros((B, NH, DHQK), dtype=k.dtype, device=k.device)
    scaM_k = torch.zeros((B, NH), dtype=k.dtype, device=k.device)
    matC_states, vecN_states, scaM_states = [], [], []
    for c in range(NC):
        matC_states.append(matC_k.to(k.dtype))
        vecN_states.append(vecN_k.to(k.dtype))
        scaM_states.append(scaM_k.to(k.dtype))
        if c == NC - 1:
            break
        scaM_k_next = torch.max(scaG[:, :, c] + scaM_k, scaA_max[:, :, c])
        matK_gated = (
            matK[:, :, c]
            * torch.exp(vecA[:, :, c, :] - scaM_k_next[..., None])[:, :, :, None]
        )
        scaGbar_k = torch.exp(scaG[:, :, c] + scaM_k - scaM_k_next)[:, :, None]
        matC_k = (
            scaGbar_k[..., None] * matC_k + matK_gated.transpose(-2, -1) @ matV[:, :, c]
        )
        vecN_k = scaGbar_k * vecN_k + matK_gated.transpose(-2, -1).sum(-1)
        scaM_k = scaM_k_next
    matC_states = torch.stack(matC_states, dim=2)  # (B, NH, NC, DHQK, DHHV)
    vecN_states = torch.stack(vecN_states, dim=2)  # (B, NH, NC, DHQK)
    scaM_states = torch.stack(scaM_states, dim=2)  # (B, NH, NC)

    # Compute the outputs within each chunk in parallel.
    ltr = torch.tril(torch.ones((L, L), dtype=torch.bool, device=q.device))
    matLogD = (
        torch.where(ltr, vecB[:, :, :, :, None] - vecB[:, :, :, None, :], -float("inf"))
        + vecI[:, :, :, None, :]
    )
    vecM_inter = vecB + scaM_states[:, :, :, None]
    vecM_combine = torch.maximum(vecM_inter, matLogD.max(dim=-1).values)[
        ..., None
    ]  # (B, NH, NC, L, 1)
    vecM_inter = vecM_inter[..., None]

    matD = torch.exp(matLogD - vecM_combine)
    matM = (matQ @ matK.transpose(-2, -1)) * qk_scale * matD
    matQ_gated = matQ * torch.exp(vecM_inter - vecM_combine) * qk_scale

    matNumerator = matQ_gated @ matC_states + matM @ matV
    vecDenom = matQ_gated @ vecN_states.unsqueeze(-1) + matM.sum(dim=-1, keepdim=True)
    vecDenom_max = torch.maximum(torch.abs(vecDenom), torch.exp(-vecM_combine))
    matH = matNumerator / (vecDenom_max + eps)
    return matH.reshape(B, NH, NC * L, DHHV)[:, :, :S, :]
