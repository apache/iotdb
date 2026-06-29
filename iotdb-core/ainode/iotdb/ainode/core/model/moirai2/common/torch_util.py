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

from typing import Optional

import numpy as np
import torch
from jaxtyping import Bool, Float, Int

numpy_to_torch_dtype_dict = {
    bool: torch.bool,
    np.uint8: torch.uint8,
    np.int8: torch.int8,
    np.int16: torch.int16,
    np.int32: torch.int32,
    np.int64: torch.int64,
    np.float16: torch.float16,
    np.float32: torch.float32,
    np.float64: torch.float64,
    np.complex64: torch.complex64,
    np.complex128: torch.complex128,
}


def packed_attention_mask(
    sample_id: Int[torch.Tensor, "*batch seq_len"],
) -> Bool[torch.Tensor, "*batch seq_len seq_len"]:
    sample_id = sample_id.unsqueeze(-1)
    attention_mask = sample_id.eq(sample_id.mT)
    return attention_mask


def packed_causal_attention_mask(
    sample_id: Int[torch.Tensor, "*batch seq_len"],
    time_id: Int[torch.Tensor, "*batch seq_len"],
) -> Bool[torch.Tensor, "*batch seq_len seq_len"]:
    attention_mask = packed_attention_mask(sample_id)
    expanded_id1 = time_id.unsqueeze(-2)
    expanded_id2 = time_id.unsqueeze(-1)
    compare_res = expanded_id1 <= expanded_id2
    attention_mask = attention_mask * compare_res
    return attention_mask


def mask_fill(
    tensor: Float[torch.Tensor, "*batch dim"],
    mask: Bool[torch.Tensor, "*batch"],
    value: Float[torch.Tensor, "dim"],
) -> Float[torch.Tensor, "*batch dim"]:
    mask = mask.unsqueeze(-1)
    return tensor * ~mask + value * mask


def safe_div(
    numer: torch.Tensor,
    denom: torch.Tensor,
) -> torch.Tensor:
    return numer / torch.where(
        denom == 0,
        1.0,
        denom,
    )


def size_to_mask(
    max_size: int,
    sizes: Int[torch.Tensor, "*batch"],
) -> Bool[torch.Tensor, "*batch max_size"]:
    mask = torch.arange(max_size, device=sizes.device)
    return torch.lt(mask, sizes.unsqueeze(-1))


def fixed_size(
    value: Float[torch.Tensor, "*batch max_size"],
) -> Int[torch.Tensor, "*batch"]:
    sizes = torch.ones_like(value[..., 0], dtype=torch.long) * value.shape[-1]
    return sizes


def sized_mean(
    value: Float[torch.Tensor, "*batch max_size"],
    sizes: Optional[Int[torch.Tensor, "*batch"]],
    dim: Optional[int | tuple[int, ...]] = None,
    keepdim: bool = False,
    size_keepdim: bool = False,
    correction: int = 0,
) -> Float[torch.Tensor, "..."]:
    value = value * size_to_mask(value.shape[-1], sizes)
    div_val = safe_div(
        value.sum(dim=-1).sum(dim, keepdim=keepdim),
        torch.clamp(sizes.sum(dim, keepdim=keepdim) - correction, min=0),
    )
    if size_keepdim:
        div_val = div_val.unsqueeze(-1)
    return div_val


def masked_mean(
    value: Float[torch.Tensor, "..."],
    mask: Bool[torch.Tensor, "..."],
    dim: Optional[int | tuple[int, ...]] = None,
    keepdim: bool = False,
    correction: int = 0,
) -> Float[torch.Tensor, "..."]:
    return safe_div(
        (value * mask).sum(dim=dim, keepdim=keepdim),
        torch.clamp(mask.float().sum(dim, keepdim=keepdim) - correction, min=0),
    )


def unsqueeze_trailing_dims(x: torch.Tensor, shape: torch.Size) -> torch.Tensor:
    if x.ndim > len(shape) or x.shape != shape[: x.ndim]:
        raise ValueError
    dim = (...,) + (None,) * (len(shape) - x.ndim)
    return x[dim]
