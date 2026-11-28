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

"""sLSTM and mLSTM recurrence kernels. Import the modules directly; the triton ones need triton."""

from functools import cache

import torch


@cache
def triton_available() -> bool:
    """Whether the Triton kernels can be used; PyTorch's Windows wheels, for one, ship without Triton.

    Triton compiles a kernel from its Python source, so the kernel modules are imported here as well:
    they fail to import where Triton is installed but the source is not, as in a frozen executable.
    """
    try:
        import triton  # noqa: F401

        from iotdb.ainode.core.model.tirex2.component.kernel import (  # noqa: F401
            mlstm_triton,
            slstm_triton,
        )
    except Exception:
        return False
    return True


# The Triton kernels multiply bfloat16 operands with tl.dot, which needs compute capability 8.0
# (Ampere) or newer. Older GPUs, such as Volta and Turing, use the PyTorch kernels.
_MIN_TRITON_CAPABILITY = (8, 0)
# The kernels' tl.dot needs operands at least 16 wide, so smaller heads run on the PyTorch kernels.
_MIN_TRITON_HEAD_DIM = 16


@cache
def _supports_triton_kernels(device_index: int) -> bool:
    return torch.cuda.get_device_capability(device_index) >= _MIN_TRITON_CAPABILITY


def use_triton(x: torch.Tensor, *head_dims: int) -> bool:
    """Whether to run the recurrence of ``x`` on the Triton kernels.

    That is the case for CUDA inputs on a GPU of compute capability 8.0 or newer, when Triton is available
    and every head dimension in ``head_dims`` is at least 16.
    """
    if any(d < _MIN_TRITON_HEAD_DIM for d in head_dims):
        return False
    if not (x.is_cuda and triton_available()):
        return False
    index = x.device.index
    return _supports_triton_kernels(
        torch.cuda.current_device() if index is None else index
    )
