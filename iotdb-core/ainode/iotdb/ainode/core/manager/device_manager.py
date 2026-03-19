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

import torch
import torch.distributed as dist

from iotdb.ainode.core.device.backend.base import BackendAdapter, BackendType
from iotdb.ainode.core.device.backend.cpu_backend import CPUBackend
from iotdb.ainode.core.device.backend.cuda_backend import CUDABackend
from iotdb.ainode.core.device.device_utils import DeviceLike, parse_device_like
from iotdb.ainode.core.device.env import DistEnv, read_dist_env
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.util.decorator import singleton
from timecho.ainode.core.device.backend.npu_backend import NPUBackend

LOGGER = Logger()


@singleton
class DeviceManager:
    use_local_rank_if_distributed: bool = True

    """
    Unified device entry point:
    - Select backend (cuda/cpu)
    - Parse device expression (None/int/str/torch.device/DeviceSpec)
    - Provide device, autocast, grad scaler, synchronize, dist backend recommendation, etc.
    """

    def __init__(self):
        self.env: DistEnv = read_dist_env()

        self.backends: dict[BackendType, BackendAdapter] = {
            BackendType.CUDA: CUDABackend(),
            BackendType.CPU: CPUBackend(),
            BackendType.NPU: NPUBackend(),
        }

        self.type: BackendType
        self.backend: BackendAdapter = self._auto_select_backend()

    # ==================== selection ====================
    def _auto_select_backend(self) -> BackendAdapter:
        for name in BackendType:
            if name == BackendType.CPU:
                # Defer CPU selection to the fallback below.
                continue
            backend = self.backends.get(name)
            if backend is not None and backend.is_available():
                self.type = backend.type
                LOGGER.info(f"AINode selects backend: {backend.type.value}")
                return backend
        LOGGER.info("No GPU backend available, AINode falling back to CPU backend.")
        backend = self.backends[BackendType.CPU]
        self.type = backend.type
        return backend

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
        Args:
            device: Could be any of the following formats:
                an integer (e.g., 0, 1, ...),
                a string (e.g., "0", "cuda:0", "cpu", ...),
                a torch.device object, return itself if so.
        Raise:
            ValueError: If device is None or incorrect.
        """
        if device is None:
            raise ValueError(
                "Device must be specified explicitly; None is not allowed."
            )
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

    # ==================== tuning API ====================

    def set_device(self, index: int) -> None:
        """Set device for current backend; CPU is no-op."""
        if self.backend.type == BackendType.CPU:
            return
        self.backend.set_device(index)

    def _current_dist_backend(self) -> str:
        """Recommend torch.distributed backend name for current device type."""
        if self.backend.type == BackendType.CPU:
            return "gloo"
        if self.backend.type == BackendType.NPU:
            return "hccl" if hasattr(torch, "npu") else "nccl"
        return "nccl"

    def init_process_group(
        self, rank: int, world_size: int, master_addr: str, master_port: str
    ) -> None:
        """Initialize torch process group using backend chosen by device manager."""
        dist.init_process_group(
            backend=self._current_dist_backend(),
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=world_size,
            rank=rank,
            device_id=self.torch_device(rank),
        )

    def destroy_process_group(self) -> None:
        if dist.is_initialized():
            dist.destroy_process_group()

    def ddp_device_ids(self, device_index: int) -> list[int] | None:
        """Return device_ids list for DDP wrapper. CPU returns None (DDP will use cpu)."""
        if self.backend.type == BackendType.CPU:
            return None
        return [device_index]

    def barrier(self):
        if dist.is_initialized():
            dist.barrier()

    def reduce(self, tensor: torch.Tensor, dst: int, op=dist.ReduceOp.SUM):
        if dist.is_initialized():
            dist.reduce(tensor, dst=dst, op=op)

    def empty_cache(self) -> None:
        """Release cached memory for current backend if supported."""
        if self.backend.type == BackendType.CUDA and hasattr(torch, "cuda"):
            torch.cuda.empty_cache()
        elif self.backend.type == BackendType.NPU and hasattr(torch, "npu"):
            torch.npu.empty_cache()
