import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from standalone_finetune.config import get_exp_dir, get_models_dir
from standalone_finetune.model.model_constants import ModelCategory, ModelStates
from standalone_finetune.model.model_info import ModelInfo
from standalone_finetune.finetune.task.task_constants import (
    CancelReason,
    TaskError,
    TaskProgress,
    TaskStatus,
)
from standalone_finetune.hparams.data_args import DataArguments
from standalone_finetune.hparams.finetune_args import FinetuneArguments
from standalone_finetune.hparams.model_args import ModelArguments
from standalone_finetune.hparams.training_args import TrainingArguments


@dataclass
class FinetuneTask:
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    base_model_info: Optional[ModelInfo] = field(default=None)
    model_args: ModelArguments = field(default_factory=ModelArguments)
    data_args: DataArguments = field(default_factory=DataArguments)
    training_args: TrainingArguments = field(default_factory=TrainingArguments)
    finetune_args: FinetuneArguments = field(default_factory=FinetuneArguments)

    status: TaskStatus = field(default=TaskStatus.PENDING)
    priority: str = field(default="normal")

    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: Optional[str] = field(default=None)
    completed_at: Optional[str] = field(default=None)

    progress: TaskProgress = field(default_factory=TaskProgress)
    error: Optional[TaskError] = field(default=None)

    pid: Optional[int] = field(default=None)
    cancel_reason: Optional[CancelReason] = field(default=None)

    @property
    def exp_dir(self) -> str:
        return os.path.join(get_exp_dir(), self.task_id)

    @property
    def output_model_dir(self) -> str:
        return os.path.join(
            get_models_dir(), ModelCategory.FINE_TUNED.value, self.model_args.model_id
        )

    @classmethod
    def create(
        cls,
        base_model_info: ModelInfo,
        model_args: ModelArguments,
        data_args: DataArguments,
        training_args: TrainingArguments,
        finetune_args: FinetuneArguments,
    ) -> "FinetuneTask":
        task_id = model_args.model_id or str(uuid.uuid4())[:8]
        return cls(
            task_id=task_id,
            base_model_info=base_model_info,
            model_args=model_args,
            data_args=data_args,
            training_args=training_args,
            finetune_args=finetune_args,
            priority=finetune_args.priority,
        )

    def start(self, pid: Optional[int] = None) -> None:
        self.status = TaskStatus.RUNNING
        self.started_at = datetime.now().isoformat()
        self.pid = pid

    def complete(self, success: bool = True, error_msg: str = "") -> None:
        self.status = TaskStatus.SUCCEEDED if success else TaskStatus.FAILED
        self.completed_at = datetime.now().isoformat()
        if not success and error_msg:
            self.error = TaskError(error_message=error_msg)

    def cancel(self, reason: CancelReason = CancelReason.USER_REQUEST) -> None:
        self.status = TaskStatus.CANCELED
        self.completed_at = datetime.now().isoformat()
        self.cancel_reason = reason

    def update_progress(self, **kwargs) -> None:
        for key, value in kwargs.items():
            if hasattr(self.progress, key):
                setattr(self.progress, key, value)

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELED,
        )

    @property
    def elapsed_seconds(self) -> Optional[float]:
        """Wall-clock seconds since task started (or total if finished)."""
        if self.started_at is None:
            return None
        start = datetime.fromisoformat(self.started_at)
        end = (
            datetime.fromisoformat(self.completed_at)
            if self.completed_at
            else datetime.now()
        )
        return (end - start).total_seconds()

    def to_summary(self) -> str:
        elapsed = self.elapsed_seconds
        elapsed_str = f"{elapsed:.0f}s" if elapsed is not None else "-"
        loss_str = (
            f"loss={self.progress.train_loss:.4f}"
            if self.progress.train_loss > 0
            else ""
        )
        return (
            f"Task: {self.task_id} | Status: {self.status.value} | "
            f"Type: {self.finetune_args.finetune_type} | "
            f"Progress: {self.progress.current_epoch}/{self.progress.total_epochs} | "
            f"Elapsed: {elapsed_str}" + (f" | {loss_str}" if loss_str else "")
        )

    def to_detail(self) -> str:
        lines = [
            f"Task ID: {self.task_id}",
            f"Status: {self.status.value}",
            f"Priority: {self.priority}",
            f"Finetune Type: {self.finetune_args.finetune_type}",
            f"Model Type: {self.model_args.model_type}",
            f"Base Model: {self.model_args.base_model_id}",
            f"Created: {self.created_at}",
        ]
        if self.started_at:
            lines.append(f"Started: {self.started_at}")
        if self.completed_at:
            lines.append(f"Completed: {self.completed_at}")
        elapsed = self.elapsed_seconds
        if elapsed is not None:
            lines.append(f"Elapsed: {elapsed:.1f}s")
        if self.progress.total_epochs > 0:
            lines.append(
                f"Progress: Epoch {self.progress.current_epoch}/{self.progress.total_epochs}, "
                f"Step {self.progress.current_step}/{self.progress.total_steps}"
            )
            if self.progress.train_loss > 0:
                lines.append(f"Train Loss: {self.progress.train_loss:.6f}")
            if self.progress.val_loss > 0:
                lines.append(f"Val Loss: {self.progress.val_loss:.6f}")
        if self.error:
            lines.append(f"Error: {self.error.error_message}")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        base_model_info_dict = None
        if self.base_model_info is not None:
            base_model_info_dict = {
                "model_id": self.base_model_info.model_id,
                "model_type": self.base_model_info.model_type,
                "category": self.base_model_info.category.value,
                "state": self.base_model_info.state.value,
                "pipeline_cls": self.base_model_info.pipeline_cls,
                "repo_id": self.base_model_info.repo_id,
                "auto_map": self.base_model_info.auto_map,
                "transformers_registered": self.base_model_info.transformers_registered,
            }
        return {
            "task_id": self.task_id,
            "base_model_info": base_model_info_dict,
            "model_args": self.model_args.to_dict(),
            "data_args": self.data_args.to_dict(),
            "training_args": self.training_args.to_dict(),
            "finetune_args": self.finetune_args.to_dict(),
            "status": self.status.value,
            "priority": self.priority,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress": {
                "current_epoch": self.progress.current_epoch,
                "total_epochs": self.progress.total_epochs,
                "current_step": self.progress.current_step,
                "total_steps": self.progress.total_steps,
                "train_loss": self.progress.train_loss,
                "val_loss": self.progress.val_loss,
            },
            "error": (
                {
                    "error_type": self.error.error_type,
                    "error_message": self.error.error_message,
                    "stack_trace": self.error.stack_trace,
                }
                if self.error
                else None
            ),
            "pid": self.pid,
            "cancel_reason": self.cancel_reason.value if self.cancel_reason else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FinetuneTask":
        base_model_info = None
        base_model_info_data = data.get("base_model_info")
        if base_model_info_data:
            base_model_info = ModelInfo(
                model_id=base_model_info_data.get("model_id", ""),
                model_type=base_model_info_data.get("model_type", ""),
                category=ModelCategory(
                    base_model_info_data.get("category", "FINETUNED")
                ),
                state=ModelStates(base_model_info_data.get("state", "INACTIVE")),
                pipeline_cls=base_model_info_data.get("pipeline_cls", ""),
                repo_id=base_model_info_data.get("repo_id", ""),
                auto_map=base_model_info_data.get("auto_map"),
                transformers_registered=base_model_info_data.get(
                    "transformers_registered", False
                ),
            )

        model_args_data = data.get("model_args", {})
        if model_args_data.get("model_name_or_path") is None:
            model_args_data["model_name_or_path"] = ""
        model_args = ModelArguments(**model_args_data)

        data_args = DataArguments(**data.get("data_args", {}))
        training_args = TrainingArguments(**data.get("training_args", {}))
        finetune_args = FinetuneArguments(**data.get("finetune_args", {}))

        progress_data = data.get("progress", {})
        progress = TaskProgress(
            current_epoch=progress_data.get("current_epoch", 0),
            total_epochs=progress_data.get("total_epochs", 0),
            current_step=progress_data.get("current_step", 0),
            total_steps=progress_data.get("total_steps", 0),
            train_loss=progress_data.get("train_loss", 0.0),
            val_loss=progress_data.get("val_loss", 0.0),
        )

        error = None
        error_data = data.get("error")
        if error_data:
            error = TaskError(
                error_type=error_data.get("error_type", ""),
                error_message=error_data.get("error_message", ""),
                stack_trace=error_data.get("stack_trace", ""),
            )

        cancel_reason = None
        if data.get("cancel_reason"):
            cancel_reason = CancelReason(data["cancel_reason"])

        return cls(
            task_id=data.get("task_id", str(uuid.uuid4())[:8]),
            base_model_info=base_model_info,
            model_args=model_args,
            data_args=data_args,
            training_args=training_args,
            finetune_args=finetune_args,
            status=TaskStatus(data.get("status", "pending")),
            priority=data.get("priority", "normal"),
            created_at=data.get("created_at", datetime.now().isoformat()),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            progress=progress,
            error=error,
            pid=data.get("pid"),
            cancel_reason=cancel_reason,
        )
