import inspect
import logging
import math
import os
from typing import Any, Callable, Dict, Optional, Tuple

import torch
import torch.distributed as dist
from torch.amp import GradScaler, autocast
from torch.optim.lr_scheduler import (
    ConstantLR,
    CosineAnnealingLR,
    LinearLR,
    SequentialLR,
)
from torch.utils.data import DataLoader

from standalone_finetune.model.model_constants import TRAINING_STATE
from standalone_finetune.model.model_loader import (
    restore_model_weights,
    restore_training_state,
    save_model as _save_model,
    save_training_checkpoint as _save_ckpt,
    unwrap_model as _unwrap,
)
from standalone_finetune.hparams.training_args import TrainingArguments

logger = logging.getLogger(__name__)


class Trainer:
    """
    Training loop for time series model fine-tuning.

    Supports:
        - Cooperative cancellation via cancel_fn
        - Real-time progress reporting via progress_callback
        - Gradient accumulation with correct loss scaling
        - Configurable optimizer (adamw / adam / sgd)
        - Configurable LR scheduler with warmup (cosine / linear / constant)
        - Checkpoint saving and resume
        - Mixed-precision training (AMP)
    """

    def __init__(
        self,
        model: torch.nn.Module,
        train_dataloader: DataLoader,
        val_dataloader: Optional[DataLoader],
        training_args: TrainingArguments,
        exp_dir: str,
        device: Optional[torch.device] = None,
        cancel_fn: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable[[Dict], None]] = None,
    ):
        self.model = model
        self.train_dataloader = train_dataloader
        self.val_dataloader = val_dataloader
        self.training_args = training_args
        self.exp_dir = exp_dir
        self.device = device or next(model.parameters()).device

        self._cancel_fn = cancel_fn
        self._progress_cb = progress_callback

        # Determine device type for AMP (cuda / cpu / npu / ...)
        self._device_type = self.device.type if self.device.type != "cpu" else "cpu"

        self.optimizer = self._create_optimizer()
        self.scheduler = self._create_scheduler()
        self.scaler = GradScaler(self._device_type) if training_args.use_amp else None

        # Inspect model forward signature once to know which params it accepts.
        # This enables a single calling path for all model types:
        #   Sundial (input_ids, labels, loss_masks, mask_y)
        #   Timer   (input_ids, labels, loss_masks)            -- no mask_y
        #   DualWeaver (input_ids, labels, loss_masks, mask_y) -- unified after refactor
        #   LoRA-wrapped models preserve the base model's signature
        self._forward_params = self._get_forward_params()

        self.global_step = 0
        self.start_epoch = 0

        os.makedirs(self.exp_dir, exist_ok=True)

    # ==================== Public API ====================

    def train(self) -> None:
        args = self.training_args
        num_epochs = args.num_train_epochs
        max_steps = args.max_steps
        accum = args.gradient_accumulation_steps
        steps_per_epoch = math.ceil(len(self.train_dataloader) / accum)

        self._report_progress(
            total_epochs=num_epochs,
            total_steps=steps_per_epoch * num_epochs,
        )

        if self._is_main_process:
            logger.info(
                f"Training: {num_epochs} epochs, "
                f"{len(self.train_dataloader)} batches/epoch, "
                f"gradient_accumulation={accum}, "
                f"effective_batch_size={args.effective_batch_size}, "
                f"max_steps={max_steps}, iter_per_epoch={args.iter_per_epoch}"
            )

        for epoch in range(self.start_epoch, num_epochs):
            if self._is_canceled():
                self._save_checkpoint("canceled")
                break

            if max_steps > 0 and self.global_step >= max_steps:
                if self._is_main_process:
                    logger.info(f"Reached max_steps ({max_steps}), stopping")
                break

            train_loss = self._train_epoch(epoch)
            if self._is_main_process:
                logger.info(
                    f"Epoch {epoch + 1}/{num_epochs} - "
                    f"Train Loss: {train_loss:.6f}, LR: {self.scheduler.get_last_lr()[0]:.2e}"
                )

            self._report_progress(
                current_epoch=epoch + 1,
                current_step=self.global_step,
                train_loss=train_loss,
            )

            # Periodic checkpoint
            save_interval = max(1, num_epochs // 5)
            if (epoch + 1) % save_interval == 0:
                self._save_checkpoint(f"epoch_{epoch + 1}")

        self._save_checkpoint("final")

    def save_model(
        self,
        output_dir: str,
        config: Any = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Save model weights and config to output_dir."""
        _save_model(self.model, output_dir, config=config, metadata=metadata)

    def resume_from_checkpoint(self, checkpoint_dir: str) -> None:
        """Restore model weights, optimizer, scheduler and step counter.

        All ranks load the same checkpoint (read-only, safe for concurrent
        access).  A barrier at the end keeps ranks synchronized before
        training resumes.
        """
        state_file = os.path.join(checkpoint_dir, TRAINING_STATE)
        if not os.path.exists(state_file):
            logger.warning(f"No training_state.pt in {checkpoint_dir}, skip resume")
            return

        # --- Restore model weights ---
        restore_model_weights(self.model, checkpoint_dir)

        # --- Restore training state (optimizer, scheduler, counters) ---
        state = restore_training_state(checkpoint_dir, self.optimizer, self.scheduler)
        self.global_step = state.get("global_step", 0)

        # Infer start epoch
        accum = self.training_args.gradient_accumulation_steps
        steps_per_epoch = math.ceil(len(self.train_dataloader) / accum)
        if steps_per_epoch > 0:
            self.start_epoch = self.global_step // steps_per_epoch

        if self._is_main_process:
            logger.info(
                f"Resumed training: step={self.global_step}, epoch={self.start_epoch}"
            )

        # Ensure all ranks have loaded before training starts
        self._barrier()

    # ==================== Training Loop ====================

    def _train_epoch(self, epoch: int) -> float:
        self.model.train()
        args = self.training_args
        accum_steps = args.gradient_accumulation_steps
        max_steps = args.max_steps
        iter_per_epoch = args.iter_per_epoch
        total_loss = 0.0
        num_micro_batches = 0
        epoch_opt_steps = 0

        # DistributedSampler must know the epoch for correct shuffling.
        # Without this, every rank receives the same data order each epoch.
        sampler = getattr(self.train_dataloader, "sampler", None)
        if sampler is not None and hasattr(sampler, "set_epoch"):
            sampler.set_epoch(epoch)

        self.optimizer.zero_grad(set_to_none=True)

        for batch_idx, batch in enumerate(self.train_dataloader):
            if self._is_canceled():
                break

            if max_steps > 0 and self.global_step >= max_steps:
                break

            if iter_per_epoch is not None and epoch_opt_steps >= iter_per_epoch:
                break

            loss_val = self._forward_backward(batch, accum_steps)
            total_loss += loss_val
            num_micro_batches += 1

            # Optimizer step every accum_steps micro-batches, or at the last batch
            if (batch_idx + 1) % accum_steps == 0 or (batch_idx + 1) == len(
                self.train_dataloader
            ):
                self._optimizer_step()
                self.optimizer.zero_grad(set_to_none=True)
                self.global_step += 1
                epoch_opt_steps += 1

                if (
                    self.global_step % args.logging_steps == 0
                    and self._is_main_process
                ):
                    avg = total_loss / num_micro_batches
                    logger.info(f"Step {self.global_step} - Loss: {avg:.6f}")

        return total_loss / max(num_micro_batches, 1)

    # ==================== Forward / Backward ====================

    def _forward_backward(self, batch, accum_steps: int) -> float:
        """Forward pass, extract loss, scale by accumulation, backward. Returns unscaled loss."""
        batch_x, batch_y, loss_mask, mask_y = self._unpack_batch(batch)

        if self.scaler is not None:
            with autocast(self._device_type):
                outputs = self._call_model(batch_x, batch_y, loss_mask, mask_y)
                loss = self._extract_loss(outputs)
            self.scaler.scale(loss / accum_steps).backward()
        else:
            outputs = self._call_model(batch_x, batch_y, loss_mask, mask_y)
            loss = self._extract_loss(outputs)
            (loss / accum_steps).backward()

        return loss.item()

    def _call_model(self, batch_x, batch_y, loss_mask, mask_y):
        """
        Call model.forward() with a unified interface.

        All model types (Sundial, Timer, DualWeaver, LoRA-wrapped, etc.)
        now share the HuggingFace-style calling convention:
            model(input_ids=..., labels=..., loss_masks=..., [mask_y=...])

        Only parameters that the model's forward() actually accepts are
        passed, preventing TypeErrors when a model lacks certain params
        (e.g. Timer has no ``mask_y``).
        """
        kwargs = {
            "input_ids": batch_x,
            "labels": batch_y,
        }
        if loss_mask is not None:
            kwargs["loss_masks"] = loss_mask
        if mask_y is not None and self._accepts("mask_y"):
            kwargs["mask_y"] = mask_y
        return self.model(**kwargs)

    def _get_forward_params(self) -> tuple:
        """Inspect model.forward() once and cache the parameter names + whether **kwargs is present."""
        model = self._unwrap_model()
        try:
            sig = inspect.signature(model.forward)
            names = set(sig.parameters.keys())
            has_var_kw = any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
            )
            return names, has_var_kw
        except (ValueError, TypeError):
            return (
                set(),
                True,
            )  # Fallback: assume **kwargs present (safe to pass anything)

    def _accepts(self, param_name: str) -> bool:
        """Check if model.forward() accepts the given parameter (by name or via **kwargs)."""
        names, has_var_kw = self._forward_params
        return param_name in names or has_var_kw

    def _optimizer_step(self) -> None:
        if self.scaler is not None:
            if self.training_args.max_grad_norm > 0:
                self.scaler.unscale_(self.optimizer)
                torch.nn.utils.clip_grad_norm_(
                    self.model.parameters(), self.training_args.max_grad_norm
                )
            self.scaler.step(self.optimizer)
            self.scaler.update()
        else:
            if self.training_args.max_grad_norm > 0:
                torch.nn.utils.clip_grad_norm_(
                    self.model.parameters(), self.training_args.max_grad_norm
                )
            self.optimizer.step()

        self.scheduler.step()

    def _unpack_batch(self, batch) -> Tuple:
        if isinstance(batch, dict):
            batch_x = batch["seq_x"].float().to(self.device)
            batch_y = batch["seq_y"].float().to(self.device)
            loss_mask = (
                batch["loss_mask"].float().to(self.device)
                if "loss_mask" in batch
                else None
            )
            mask_y = (
                batch["y_mask"].float().to(self.device) if "y_mask" in batch else None
            )
        else:
            batch_x = batch[0].float().to(self.device)
            batch_y = batch[1].float().to(self.device)
            loss_mask = batch[2].float().to(self.device) if len(batch) > 2 else None
            mask_y = (
                batch[3].float().to(self.device)
                if len(batch) > 3 and batch[3] is not None
                else None
            )
        return batch_x, batch_y, loss_mask, mask_y

    @staticmethod
    def _extract_loss(outputs) -> torch.Tensor:
        if hasattr(outputs, "loss"):
            return outputs.loss
        if isinstance(outputs, dict) and "loss" in outputs:
            return outputs["loss"]
        if isinstance(outputs, (tuple, list)):
            return outputs[0]
        return outputs

    # ==================== Optimizer & Scheduler ====================

    def _create_optimizer(self) -> torch.optim.Optimizer:
        args = self.training_args
        params = [p for p in self.model.parameters() if p.requires_grad]

        if args.optimizer_type == "adam":
            return torch.optim.Adam(
                params,
                lr=args.learning_rate,
                betas=(args.adam_beta1, args.adam_beta2),
                eps=args.adam_epsilon,
            )
        elif args.optimizer_type == "sgd":
            return torch.optim.SGD(
                params,
                lr=args.learning_rate,
                weight_decay=args.weight_decay,
            )
        else:  # adamw (default)
            return torch.optim.AdamW(
                params,
                lr=args.learning_rate,
                weight_decay=args.weight_decay,
                betas=(args.adam_beta1, args.adam_beta2),
                eps=args.adam_epsilon,
            )

    def _create_scheduler(self):
        args = self.training_args
        accum = args.gradient_accumulation_steps
        total_steps = (
            math.ceil(len(self.train_dataloader) / accum) * args.num_train_epochs
        )
        total_steps = max(total_steps, 1)

        warmup_steps = args.warmup_steps
        if args.warmup_ratio > 0:
            warmup_steps = int(total_steps * args.warmup_ratio)
        warmup_steps = min(warmup_steps, total_steps)

        decay_steps = max(total_steps - warmup_steps, 1)

        sched_type = args.scheduler_type
        if sched_type == "linear":
            main = LinearLR(
                self.optimizer,
                start_factor=1.0,
                end_factor=0.0,
                total_iters=decay_steps,
            )
        elif sched_type in ("constant", "constant_with_warmup"):
            main = ConstantLR(self.optimizer, factor=1.0, total_iters=decay_steps)
        else:  # cosine (default)
            main = CosineAnnealingLR(self.optimizer, T_max=decay_steps, eta_min=1e-8)

        if warmup_steps > 0:
            warmup = LinearLR(
                self.optimizer,
                start_factor=1e-3,
                end_factor=1.0,
                total_iters=warmup_steps,
            )
            return SequentialLR(
                self.optimizer,
                schedulers=[warmup, main],
                milestones=[warmup_steps],
            )

        return main

    # ==================== Cancel & Progress ====================

    def _is_canceled(self) -> bool:
        if self._cancel_fn is not None and self._cancel_fn():
            logger.info("Training canceled by external request")
            return True
        return False

    def _report_progress(self, **kwargs) -> None:
        if self._progress_cb is not None:
            self._progress_cb(kwargs)

    # ==================== Checkpointing ====================

    def _save_checkpoint(self, name: str) -> None:
        """Save a training checkpoint.

        In DDP only rank 0 performs the actual file I/O.  A barrier at the
        end ensures all ranks stay synchronized before proceeding.
        """
        checkpoint_dir = os.path.join(self.exp_dir, "checkpoints", name)

        if self._is_main_process:
            os.makedirs(checkpoint_dir, exist_ok=True)
            _save_ckpt(
                self.model,
                checkpoint_dir,
                self.optimizer,
                self.scheduler,
                self.global_step,
                float("inf"),
            )

        # All ranks wait until rank 0 finishes writing
        self._barrier()

    def _unwrap_model(self) -> torch.nn.Module:
        return _unwrap(self.model)

    # ==================== DDP Helpers ====================

    @property
    def _is_main_process(self) -> bool:
        """True when running single-process or when this is DDP rank 0."""
        if dist.is_available() and dist.is_initialized():
            return dist.get_rank() == 0
        return True

    def _barrier(self) -> None:
        """Synchronize all DDP ranks (no-op outside DDP)."""
        if dist.is_available() and dist.is_initialized():
            dist.barrier()
