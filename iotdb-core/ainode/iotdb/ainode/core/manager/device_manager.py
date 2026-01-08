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

import torch

from iotdb.ainode.core.device.backend.base import BackendAdapter, BackendType
from iotdb.ainode.core.device.backend.cpu_backend import CPUBackend
from iotdb.ainode.core.device.backend.cuda_backend import CUDABackend
from iotdb.ainode.core.device.device_utils import DeviceLike, parse_device_like
from iotdb.ainode.core.device.env import DistEnv, read_dist_env
from iotdb.ainode.core.util.decorator import singleton


@singleton
class DeviceManager:
    use_local_rank_if_distributed: bool = True

    """
    Unified device entry point:
    - Select backend (cuda/npu/cpu)
    - Parse device expression (None/int/str/torch.device/DeviceSpec)
    - Provide device, autocast, grad scaler, synchronize, dist backend recommendation, etc.
    """

    def __init__(self):
        self.env: DistEnv = read_dist_env()

        self.backends: dict[BackendType, BackendAdapter] = {
            BackendType.CUDA: CUDABackend(),
            BackendType.CPU: CPUBackend(),
        }

        self.type: BackendType
        self.backend: BackendAdapter = self._auto_select_backend()
        self.default_index: Optional[int] = self._select_default_index()

        # ensure process uses correct device early
        self._set_device_for_process()
        self.device: torch.device = self.backend.make_device(self.default_index)

    # ==================== selection ====================
    def _auto_select_backend(self) -> BackendAdapter:
        for name in BackendType:
            backend = self.backends.get(name)
            if backend is not None and backend.is_available():
                self.type = backend.type
                return backend
        return self.backends[BackendType.CPU]

    def _select_default_index(self) -> Optional[int]:
        if self.backend.type == BackendType.CPU:
            return None
        if self.use_local_rank_if_distributed and self.env.world_size > 1:
            return self.env.local_rank
        return 0

    def _set_device_for_process(self) -> None:
        if self.backend.type in (BackendType.CUDA,) and self.default_index is not None:
            self.backend.set_device(self.default_index)

    # ==================== public API ====================
    def device_ids(self) -> list[int]:
        """
        Returns a list of available device IDs for the current backend.
        """
        if self.backend.type == BackendType.CPU:
            return []
        return list(range(self.backend.device_count()))

    def available_devices_with_cpu(self) -> list[torch.device]:
        """
        Returns the list of available torch.devices, including "cpu".
        """
        device_id_list = self.device_ids()
        device_id_list = [self.torch_device(device_id) for device_id in device_id_list]
        device_id_list.append(self.torch_device("cpu"))
        return device_id_list

    def torch_device(self, device: DeviceLike) -> torch.device:
        """
        Convert a DeviceLike specification into a torch.device object.
        If device is None, returns the default device of current process.
        Args:
            device: Could be any of the following formats:
                an integer (e.g., 0, 1, ...),
                a string (e.g., "0", "cuda:0", "cpu", ...),
                a torch.device object, return itself if so.
        """
        if device is None:
            return self.device
        if isinstance(device, torch.device):
            return device
        spec = parse_device_like(device)
        if spec.type == "cpu":
            return torch.device("cpu")
        return self.backend.make_device(spec.index)

    def move_model(
        self, model: torch.nn.Module, device: DeviceLike = None
    ) -> torch.nn.Module:
        return model.to(self.torch_device(device))

    def move_tensor(
        self, tensor: torch.Tensor, device: DeviceLike = None
    ) -> torch.Tensor:
        return tensor.to(self.torch_device(device))
