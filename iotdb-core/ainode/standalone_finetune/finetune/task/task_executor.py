import os
import socket
import traceback
import warnings
from typing import Any, Dict, Optional

import torch
import torch.multiprocessing as mp

import logging

from standalone_finetune.device.backend import DeviceManager
from standalone_finetune.model.model_constants import ModelCategory
from standalone_finetune.model.model_loader import load_pretrained_for_finetune
from standalone_finetune.data_provider.data_factory import create_dataloader, create_dataset
from standalone_finetune.finetune.adapter.adapter import apply_adaptation
from standalone_finetune.finetune.task.task_info import FinetuneTask
from standalone_finetune.finetune.train.trainer import Trainer

logger = logging.getLogger(__name__)
BACKEND = DeviceManager()


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _training_worker(
    rank: int,
    world_size: int,
    task: FinetuneTask,
    master_addr: str,
    master_port: int,
    result_dict: Dict[str, Any],
    cancel_event: Optional[mp.Event] = None,
) -> None:
    try:
        # Suppress PyTorch internal DeprecationWarning from pin_memory.py
        # (Tensor.pin_memory() / Tensor.is_pinned() device arg deprecated)
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            module=r"torch\.utils\.data\._utils\.pin_memory",
        )

        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(master_port)
        os.environ["RANK"] = str(rank)
        os.environ["WORLD_SIZE"] = str(world_size)
        os.environ["LOCAL_RANK"] = str(rank)

        BACKEND.init_process_group(rank, world_size, master_addr, str(master_port))
        BACKEND.set_device(rank)

        cur_device = (
            BACKEND.backend.make_device(rank)
            if BACKEND.device_ids()
            else torch.device("cpu")
        )

        model_args = task.model_args
        data_args = task.data_args
        training_args = task.training_args
        finetune_args = task.finetune_args

        # ---- Model ----
        model, config = load_pretrained_for_finetune(task.base_model_info, model_args)
        model = apply_adaptation(model, model_args, finetune_args)
        model = model.to(cur_device)

        if world_size > 1:
            model = torch.nn.parallel.DistributedDataParallel(
                model,
                device_ids=BACKEND.ddp_device_ids(rank),
            )

        # ---- Data ----
        distributed = world_size > 1

        train_dataset = create_dataset(model_args, data_args, flag="train")
        train_loader = create_dataloader(
            train_dataset,
            batch_size=training_args.train_batch_size,
            shuffle=True,
            num_workers=data_args.num_workers,
            pin_memory=data_args.pin_memory,
            drop_last=True,
            distributed=distributed,
        )

        # ---- Cancel / Progress hooks ----
        def is_canceled() -> bool:
            return cancel_event is not None and cancel_event.is_set()

        def on_progress(info: dict) -> None:
            for k, v in info.items():
                result_dict[k] = v

        # ---- Trainer ----
        trainer = Trainer(
            model=model,
            train_dataloader=train_loader,
            val_dataloader=None,
            training_args=training_args,
            exp_dir=task.exp_dir,
            device=cur_device,
            cancel_fn=is_canceled,
            progress_callback=on_progress,
        )

        # Optional: resume from checkpoint
        if finetune_args.finetune_from_checkpoint:
            trainer.resume_from_checkpoint(finetune_args.finetune_from_checkpoint)

        trainer.train()

        # ---- Save final model (rank 0 only) ----
        if rank == 0 and not is_canceled():
            metadata = {
                "model_id": task.model_args.model_id,
                "base_model_id": task.model_args.base_model_id,
                "category": ModelCategory.FINE_TUNED.value,
                "pipeline_cls": (
                    task.base_model_info.pipeline_cls if task.base_model_info else ""
                ),
            }
            trainer.save_model(task.output_model_dir, config, metadata=metadata)
            result_dict["success"] = True
        elif is_canceled():
            result_dict["canceled"] = True

        BACKEND.destroy_process_group()

    except Exception as e:
        logger.error(f"Training worker {rank} failed: {e}")
        traceback.print_exc()
        result_dict["success"] = False
        result_dict["error"] = str(e)

        try:
            BACKEND.destroy_process_group()
        except Exception:
            pass


class TaskExecutor:
    """
    Manages the lifecycle of a single training run.

    Handles both single-GPU and distributed (DDP) execution.
    Accepts an optional cancel_event (mp.Event) that, when set, signals
    the training worker(s) to stop cooperatively.
    """

    def execute(
        self,
        task: FinetuneTask,
        cancel_event: Optional[mp.Event] = None,
    ) -> Dict[str, Any]:
        training_args = task.training_args

        if training_args.ddp and training_args.world_size > 1:
            return self._execute_distributed(task, cancel_event)
        else:
            return self._execute_single(task, cancel_event)

    def _execute_single(
        self,
        task: FinetuneTask,
        cancel_event: Optional[mp.Event] = None,
    ) -> Dict[str, Any]:
        result_dict = mp.Manager().dict({"success": False})

        _training_worker(
            rank=0,
            world_size=1,
            task=task,
            master_addr="localhost",
            master_port=_find_free_port(),
            result_dict=result_dict,
            cancel_event=cancel_event,
        )

        return dict(result_dict)

    def _execute_distributed(
        self,
        task: FinetuneTask,
        cancel_event: Optional[mp.Event] = None,
    ) -> Dict[str, Any]:
        world_size = task.training_args.world_size
        master_addr = "localhost"
        master_port = _find_free_port()

        result_dict = mp.Manager().dict({"success": False})

        mp.spawn(
            _training_worker,
            args=(
                world_size,
                task,
                master_addr,
                master_port,
                result_dict,
                cancel_event,
            ),
            nprocs=world_size,
            join=True,
        )

        return dict(result_dict)
