import torch

from iotdb.ainode.core.device.backend.base import BackendAdapter, BackendType


class NPUBackend(BackendAdapter):
    type = BackendType.NPU

    def is_available(self) -> bool:
        # return hasattr(torch, "npu") and torch.npu.is_available()
        return False

    def device_count(self) -> int:
        # return torch.npu.device_count()
        return 0

    def make_device(self, index: int | None) -> torch.device:
        # if index is None:
        #     raise ValueError("NPU backend requires a valid device index")
        # return torch.device(f"npu:{index}")
        return torch.device("cpu")

    def set_device(self, index: int) -> None:
        # torch.npu.set_device(index)
        pass
