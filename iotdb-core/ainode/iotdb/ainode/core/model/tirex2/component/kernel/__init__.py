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


def use_triton(x: torch.Tensor) -> bool:
    """Whether to run the recurrence of ``x`` on the Triton kernels: on CUDA, when they are available."""
    return x.is_cuda and triton_available()
