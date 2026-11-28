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

"""Triton sLSTM forward kernel, used for the recurrence on CUDA.

Vendored from ``slstm_kernels/triton_kernel/slstm_triton.py`` in
https://github.com/NX-AI/slstm-kernels at commit ``b2bf942``, with int64 batch
offsets. Keep the two in sync; changes to the kernel belong upstream first.

Unlike FlashRNN's CUDA kernel, it keeps the cell, normalizer and stabilizer
states in float32 across time steps and launches on the current stream without
synchronizing, so the recurrence can be captured in a CUDA graph. Only the
recurrent matmul runs on bfloat16 operands.
"""

from functools import cache

import torch
import triton
import triton.language as tl
from triton.language.extra import libdevice

from iotdb.ainode.core.model.tirex2.component.kernel.slstm_torch import slstm_torch

NG_GATES = 4

LOG2E = tl.constexpr(1.4426950408889634)

SMEM_HEADROOM = 4096


@cache
def _max_shared_mem(device_index: int) -> int:
    return triton.runtime.driver.active.utils.get_device_properties(device_index)[
        "max_shared_mem"
    ]


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


@triton.jit
def slstm_pointwise(iraw, fraw, zraw, oraw, c, n, m):
    logfplusm = m - tl.log(1.0 + tl.exp(-tl.minimum(fraw, 15.0)))
    mnew = tl.where(n == 0.0, iraw, tl.maximum(iraw, logfplusm))
    igate = tl.minimum(tl.exp2(LOG2E * (iraw - mnew)), 1.0)
    fgate = tl.minimum(tl.exp2(LOG2E * (logfplusm - mnew)), 1.0)
    zgate = libdevice.tanh(zraw)
    cnew = fgate * c + igate * zgate
    nnew = tl.maximum((fgate * n + igate), 1.0)
    hnew = cnew / (nnew * (1.0 + tl.exp2(-LOG2E * oraw)))
    return hnew, cnew, nnew, mnew


@triton.autotune(
    configs=[
        triton.Config({"B_BLOCK_SIZE": 2**i}, num_warps=w, num_stages=1)
        for i in range(6)
        for w in (4, 8, 16)
    ],
    key=["B_POW2", "NH", "NG", "HD", "RESIDENT_WEIGHTS"],
    prune_configs_by={
        "early_config_prune": lambda configs, nargs, **kwargs: [
            c for c in configs if c.kwargs["B_BLOCK_SIZE"] <= nargs["B_POW2"]
        ]
    },
)
@triton.jit
def slstm_kernel(
    x_ptr,
    states_ptr,
    weight_ptr,
    bias_ptr,
    cu_seqlens_ptr,
    out,
    out_states,
    B,
    S,
    B_POW2,
    NH: tl.constexpr,
    NG: tl.constexpr,
    HD: tl.constexpr,
    IS_VARLEN: tl.constexpr,
    RESIDENT_WEIGHTS: tl.constexpr,
    B_BLOCK_SIZE: tl.constexpr,
):
    pid = tl.program_id(axis=0)
    r = tl.arange(0, HD)[None, :]
    head_id = pid % NH
    # int64, as the input/output offsets derived from it exceed int32 beyond ~14k rows of 74 steps
    batch_id = ((pid // NH) * B_BLOCK_SIZE + tl.arange(0, B_BLOCK_SIZE)[:, None]).to(
        tl.int64
    )
    batch_mask = batch_id < B

    if IS_VARLEN:
        seq_start = tl.load(cu_seqlens_ptr + batch_id, mask=batch_mask, other=0)
        seq_end = tl.load(cu_seqlens_ptr + batch_id + 1, mask=batch_mask, other=0)
        seq_len = (seq_end - seq_start).to(tl.int32)
        # Rows of the block run in lockstep, so step through the longest one and mask
        num_steps = tl.max(tl.where(batch_mask, seq_len, 0))
        x_mask = batch_mask & (seq_len > 0)
    else:
        seq_start = batch_id * S
        seq_len = S
        num_steps = S
        x_mask = batch_mask

    state_offs = batch_id * NH * NG * HD + head_id * NG * HD + r
    y = tl.load(states_ptr + state_offs + 0 * HD, mask=batch_mask, other=0.0)
    c = tl.load(states_ptr + state_offs + 1 * HD, mask=batch_mask, other=0.0)
    n = tl.load(states_ptr + state_offs + 2 * HD, mask=batch_mask, other=0.0)
    m = tl.load(states_ptr + state_offs + 3 * HD, mask=batch_mask, other=0.0)

    # weight arrives transposed, [NH, NG, HD_in, HD_out], so each gate is a row-major tile
    w_ptrs = weight_ptr + head_id * NG * HD * HD + tl.arange(0, HD)[:, None] * HD + r
    if RESIDENT_WEIGHTS:
        w_i = tl.load(w_ptrs + 0 * HD * HD)
        w_f = tl.load(w_ptrs + 1 * HD * HD)
        w_z = tl.load(w_ptrs + 2 * HD * HD)
        w_o = tl.load(w_ptrs + 3 * HD * HD)

    bias_offs = head_id * NG * HD
    b_i = tl.load(bias_ptr + bias_offs + 0 * HD + r)
    b_f = tl.load(bias_ptr + bias_offs + 1 * HD + r)
    b_z = tl.load(bias_ptr + bias_offs + 2 * HD + r)
    b_o = tl.load(bias_ptr + bias_offs + 3 * HD + r)

    x_base = x_ptr + seq_start * NH * NG * HD + head_id * NG * HD + r
    out_base = out + seq_start * NH * HD + head_id * HD + r

    p_i = tl.load(x_base + 0 * HD, mask=x_mask, other=0.0)
    p_f = tl.load(x_base + 1 * HD, mask=x_mask, other=0.0)
    p_z = tl.load(x_base + 2 * HD, mask=x_mask, other=0.0)
    p_o = tl.load(x_base + 3 * HD, mask=x_mask, other=0.0)

    for step_id in range(num_steps):
        x_i, x_f, x_z, x_o = p_i, p_f, p_z, p_o  # double buffer

        # clamped to the row's own last step in cu_seqlen mode
        next_offs = x_base + tl.minimum(step_id + 1, seq_len - 1) * NH * NG * HD
        p_i = tl.load(next_offs + 0 * HD, mask=x_mask, other=0.0)
        p_f = tl.load(next_offs + 1 * HD, mask=x_mask, other=0.0)
        p_z = tl.load(next_offs + 2 * HD, mask=x_mask, other=0.0)
        p_o = tl.load(next_offs + 3 * HD, mask=x_mask, other=0.0)

        y_bf16 = y.to(tl.bfloat16)
        if RESIDENT_WEIGHTS:
            iraw = tl.dot(y_bf16, w_i, acc=x_i.to(tl.float32) + b_i)
            fraw = tl.dot(y_bf16, w_f, acc=x_f.to(tl.float32) + b_f)
            zraw = tl.dot(y_bf16, w_z, acc=x_z.to(tl.float32) + b_z)
            oraw = tl.dot(y_bf16, w_o, acc=x_o.to(tl.float32) + b_o)
        else:
            # Re-read from L2 every step
            iraw = tl.dot(
                y_bf16, tl.load(w_ptrs + 0 * HD * HD), acc=x_i.to(tl.float32) + b_i
            )
            fraw = tl.dot(
                y_bf16, tl.load(w_ptrs + 1 * HD * HD), acc=x_f.to(tl.float32) + b_f
            )
            zraw = tl.dot(
                y_bf16, tl.load(w_ptrs + 2 * HD * HD), acc=x_z.to(tl.float32) + b_z
            )
            oraw = tl.dot(
                y_bf16, tl.load(w_ptrs + 3 * HD * HD), acc=x_o.to(tl.float32) + b_o
            )

        ynew, cnew, nnew, mnew = slstm_pointwise(iraw, fraw, zraw, oraw, c, n, m)

        if IS_VARLEN:
            # Freeze a row on its final state
            step_mask = batch_mask & (step_id < seq_len)
            y = tl.where(step_mask, ynew, y)
            c = tl.where(step_mask, cnew, c)
            n = tl.where(step_mask, nnew, n)
            m = tl.where(step_mask, mnew, m)
        else:
            step_mask = batch_mask
            y, c, n, m = ynew, cnew, nnew, mnew

        tl.store(out_base + step_id * NH * HD, y, mask=step_mask)

    tl.store(out_states + state_offs + 0 * HD, y, mask=batch_mask)
    tl.store(out_states + state_offs + 1 * HD, c, mask=batch_mask)
    tl.store(out_states + state_offs + 2 * HD, n, mask=batch_mask)
    tl.store(out_states + state_offs + 3 * HD, m, mask=batch_mask)


def slstm_triton_fw(
    x: torch.Tensor,  # [B, S, NH, NG, HD]
    states: torch.Tensor,  # [B, NH, NG, HD]
    weight: torch.Tensor,  # [NH, NG, HD, HD]
    bias: torch.Tensor,  # [NH, NG, HD]
    cu_seqlens: torch.Tensor = None,  # [B+1]
) -> tuple[torch.Tensor, torch.Tensor]:  # [B, S, NH, HD], [B, NH, NG, HD]
    if torch.is_grad_enabled() and any(
        t.requires_grad for t in (x, states, weight, bias)
    ):
        return slstm_torch(x, states, weight, bias, cu_seqlens)

    check_tensors(x, states, weight, bias, cu_seqlens)
    Bx, S, NH, NG, HD = x.shape
    B = states.shape[0]
    assert NG == NG_GATES, f"triton backend requires NG={NG_GATES} gates, got {NG}!"
    # and transposed so the kernel loads each gate as the [HD_in, HD_out] dot operand
    weight = weight.to(torch.bfloat16).transpose(-1, -2).contiguous()
    resident_weights = (
        NG * HD * HD * weight.element_size() + SMEM_HEADROOM
        <= _max_shared_mem(x.device.index)
    )

    out = torch.empty((Bx, S, NH, HD), dtype=x.dtype, device=x.device)
    out_states = torch.empty_like(states)

    grid = lambda meta: (triton.cdiv(B, meta["B_BLOCK_SIZE"]) * NH,)
    slstm_kernel[grid](
        x,
        states,
        weight,
        bias,
        cu_seqlens,
        out,
        out_states,
        B,
        S,
        triton.next_power_of_2(B),
        NH,
        NG,
        HD,
        cu_seqlens is not None,
        resident_weights,
    )

    return out, out_states
