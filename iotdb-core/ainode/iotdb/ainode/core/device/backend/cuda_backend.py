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
import time

import torch

from iotdb.ainode.core.device.backend.base import BackendAdapter, BackendType


class CUDABackend(BackendAdapter):
    type = BackendType.CUDA

    def __init__(self) -> None:
        self._safe_cuda_init()

    def is_available(self) -> bool:
        return torch.cuda.is_available()

    def device_count(self) -> int:
        return torch.cuda.device_count()

    def make_device(self, index: int | None) -> torch.device:
        if index is None:
            raise ValueError("CUDA backend requires a valid device index")
        return torch.device(f"cuda:{index}")

    def set_device(self, index: int) -> None:
        torch.cuda.set_device(index)

    def _safe_cuda_init(self) -> None:
        # Safe CUDA initialization to avoid potential deadlocks
        # This is a workaround for certain PyTorch versions where the first CUDA call can cause a long delay
        # By calling a simple CUDA operation at startup, we can ensure that the CUDA context is initialized early
        # and avoid unexpected delays during actual model loading or inference.
        attempt_cnt = 3
        for attempt in range(attempt_cnt):
            try:
                if self.is_available():
                    return
                raise RuntimeError("CUDA not available")
            except Exception as e:
                print(f"CUDA init attempt {attempt + 1} failed: {e}")
                if attempt < attempt_cnt:
                    time.sleep(1.5)
