"""Torch-only device backend (CUDA or CPU) for standalone fine-tune."""
import torch
import torch.distributed as dist


class DeviceManager:
    def __init__(self):
        self._use_cuda = torch.cuda.is_available()

    def device_ids(self):
        if not self._use_cuda:
            return []
        return list(range(torch.cuda.device_count()))

    def torch_device(self, index):
        if isinstance(index, torch.device):
            return index
        if index is None or (isinstance(index, str) and index.lower() == "cpu"):
            return torch.device("cpu")
        if self._use_cuda:
            i = int(index) if isinstance(index, str) else index
            return torch.device(f"cuda:{i}")
        return torch.device("cpu")

    def set_device(self, index: int) -> None:
        if self._use_cuda:
            torch.cuda.set_device(index)

    def init_process_group(self, rank: int, world_size: int, master_addr: str, master_port: str) -> None:
        backend = "nccl" if self._use_cuda else "gloo"
        dist.init_process_group(
            backend=backend,
            init_method=f"tcp://{master_addr}:{master_port}",
            world_size=world_size,
            rank=rank,
        )

    def destroy_process_group(self) -> None:
        if dist.is_initialized():
            dist.destroy_process_group()

    def ddp_device_ids(self, rank: int):
        if not self._use_cuda:
            return None
        return [rank]

    @property
    def backend(self):
        return self

    def make_device(self, index: int) -> torch.device:
        return self.torch_device(index)
