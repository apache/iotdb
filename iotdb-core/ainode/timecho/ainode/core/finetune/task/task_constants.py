from dataclasses import dataclass
from enum import Enum
from typing import Literal


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class CancelReason(Enum):
    USER_REQUEST = "user_request"
    TIMEOUT = "timeout"
    RESOURCE_UNAVAILABLE = "resource_unavailable"


TaskPriority = Literal["urgent", "normal", "low"]

PRIORITY_ORDER = {
    "urgent": 0,
    "normal": 1,
    "low": 2,
}


@dataclass
class TaskProgress:
    current_epoch: int = 0
    total_epochs: int = 0
    current_step: int = 0
    total_steps: int = 0
    train_loss: float = 0.0
    val_loss: float = 0.0


@dataclass
class TaskError:
    error_type: str = ""
    error_message: str = ""
    stack_trace: str = ""


# Task storage constants
SFT_HPARAMS_DIR = "sft_hparams"
UNFINISHED_DIR = "unfinished"  # Directory for pending / running task files
COMPLETED_DIR = (
    "completed"  # Directory for finished (succeeded/failed/canceled) task files
)
MANIFEST = "manifest.json"  # Persists queue runtime state (e.g. active_task_id)
TASK_FILE_EXTENSION = ".json"  # Serialization format for individual task files
TEMP_FILE_SUFFIX = ".tmp"  # Atomic-write staging suffix
