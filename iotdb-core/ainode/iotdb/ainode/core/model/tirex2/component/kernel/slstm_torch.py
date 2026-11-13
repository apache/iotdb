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

"""PyTorch sLSTM forward, used for the recurrence on CPU and MPS.

Vendored from ``slstm_kernels/torch_kernel/slstm_torch.py`` in
https://github.com/NX-AI/slstm-kernels at commit ``b2bf942``. Keep the two in
sync; changes belong upstream first. Like the Triton kernel it keeps the states
in float32 and rounds only the hidden state to bfloat16 for the recurrent matmul.
"""

import torch
import torch.nn.functional as F

NG_GATES = 4


def check_tensors(x, states, weight, bias, cu_seqlens):
    Bx, S, NH, NG, HD = x.shape
    B = states.shape[0]

    assert states.shape == (
        B,
        NH,
        NG,
        HD,
    ), f"states must have shape [B, NH, NG, HD] = {(B, NH, NG, HD)}, got {tuple(states.shape)}!"
    assert weight.shape == (
        NH,
        NG,
        HD,
        HD,
    ), f"weight must have shape [NH, NG, HD, HD] = {(NH, NG, HD, HD)}, got {tuple(weight.shape)}!"
    assert bias.shape == (
        NH,
        NG,
        HD,
    ), f"bias must have shape [NH, NG, HD] = {(NH, NG, HD)}, got {tuple(bias.shape)}!"

    assert NG == NG_GATES, f"slstm requires NG={NG_GATES} gates (i, f, z, o), got {NG}!"

    assert x.dtype in (torch.bfloat16, torch.float32), "x must be bfloat16!"
    assert states.dtype == torch.float32, "states must be float32!"
    assert weight.dtype in (torch.bfloat16, torch.float32), "weight must be bfloat16!"
    assert bias.dtype == torch.float32, "bias must be float32!"

    assert x.is_contiguous(), "x must be contiguous!"
    assert states.is_contiguous(), "states must be contiguous!"
    assert weight.is_contiguous(), "weight must be contiguous!"
    assert bias.is_contiguous(), "bias must be contiguous!"

    assert states.device == x.device, "states must be on the same device as x!"
    assert weight.device == x.device, "weight must be on the same device as x!"
    assert bias.device == x.device, "bias must be on the same device as x!"

    if cu_seqlens is None:
        assert Bx == B, "x and states must have the same batch size!"
    else:
        assert cu_seqlens.shape == (B + 1,), "cu_seqlens must have shape [B+1]!"
        assert cu_seqlens.dtype == torch.int64, "cu_seqlens must be int64!"
        assert (
            cu_seqlens.device == x.device
        ), "cu_seqlens must be on the same device as x!"
        assert cu_seqlens.is_contiguous(), "cu_seqlens must be contiguous!"


def slstm_torch(
    x: torch.Tensor,  # [B, S, NH, NG, HD]
    states: torch.Tensor,  # [B, NH, NG, HD]
    weight: torch.Tensor,  # [NH, NG, HD, HD]
    bias: torch.Tensor,  # [NH, NG, HD]
    cu_seqlens: torch.Tensor = None,  # [B+1]
) -> tuple[torch.Tensor, torch.Tensor]:  # [B, S, NH, HD], [B, NH, NG, HD]
    check_tensors(x, states, weight, bias, cu_seqlens)
    assert cu_seqlens is None, "cu_seqlens isn't implementent for torch backend!"
    B, S, NH, NG, HD = x.shape
    assert NG == NG_GATES, f"torch backend requires NG={NG_GATES} gates, got {NG}!"
    assert states.shape == (B, NH, NG, HD)
    states = states.unbind(dim=-2)

    output = []
    for i in range(S):
        y_bf16, w_t = (
            states[0].bfloat16().float(),
            weight.view(NH, NG * HD, HD).transpose(-1, -2).float(),
        )
        Ry = torch.einsum("BHE,HEO->BHO", y_bf16, w_t).view(B, NH, NG, HD)
        states = slstm_torch_pointwise(
            x[:, i].float(), Ry.float(), bias.unsqueeze(0), states
        )
        output.append(states[0].to(x.dtype))

    return torch.stack(output, dim=1), torch.stack(states, dim=-2)


def slstm_torch_pointwise(
    Wx: torch.Tensor,  # [B, NH, NG, HD]
    Ry: torch.Tensor,  # [B, NH, NG, HD]
    b: torch.Tensor,  # [1, NH, NG, HD]
    states: list[torch.Tensor],  # NG x [B, NH, HD]
) -> list[torch.Tensor]:
    raw = Wx + Ry + b
    iraw, fraw, zraw, oraw = raw.unbind(dim=-2)
    y, c, n, m = states

    # Equations reference the xlstm paper on page 4: https://arxiv.org/pdf/2405.04517
    logfplusm = m + F.logsigmoid(fraw.clamp(max=15))  # eq 15 # Clamp to avoid subnomals
    mnew = torch.where(n == 0.0, iraw, torch.max(iraw, logfplusm))  # eq 15
    ogate = torch.sigmoid(oraw)  # eq 14
    igate = torch.exp(iraw - mnew).clamp(max=1)  # eq 16
    fgate = torch.exp(logfplusm - mnew).clamp(max=1)  # eq 17
    zgate = torch.tanh(zraw)  # eq 11
    cnew = fgate * c + igate * zgate  # eq 8
    # TODO: Why do wee need the maximum here?
    nnew = (fgate * n + igate).clamp(min=1)  # eq 9
    hnew = ogate * cnew / nnew  # eq 10

    return [hnew, cnew, nnew, mnew]  # NG x [B, NH, HD]
