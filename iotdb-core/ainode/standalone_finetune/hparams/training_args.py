from dataclasses import asdict, dataclass, field
from typing import List, Literal, Optional


@dataclass
class TrainingArguments:
    """
    Arguments pertaining to training process configuration.

    This class defines parameters for:
    - Training duration (epochs, steps)
    - Batch sizes for training and validation
    - Optimization (learning rate, optimizer, scheduler)
    - Distributed training (DDP configuration)
    - Checkpointing and logging
    """

    num_train_epochs: int = field(
        default=1,
        metadata={
            "help": "Total number of training epochs.",
        },
    )

    max_steps: int = field(
        default=500,
        metadata={
            "help": (
                "Maximum number of training steps. "
                "If set to a positive number, overrides num_train_epochs."
            ),
        },
    )

    iter_per_epoch: Optional[int] = field(
        default=None,
        metadata={
            "help": (
                "Number of iterations per epoch. "
                "If None, uses full dataset each epoch."
            ),
        },
    )

    train_batch_size: int = field(
        default=32,
        metadata={
            "help": "Training batch size per device.",
        },
    )

    val_batch_size: int = field(
        default=32,
        metadata={
            "help": "Validation/evaluation batch size on per device.",
        },
    )

    gradient_accumulation_steps: int = field(
        default=1,
        metadata={
            "help": (
                "Number of gradient accumulation steps. "
                "Effective batch size = micro_batch_size * gradient_accumulation_steps * num_gpus."
            ),
        },
    )

    learning_rate: float = field(
        default=1e-5,
        metadata={
            "help": "Initial learning rate for optimizer.",
        },
    )

    weight_decay: float = field(
        default=0.1,
        metadata={
            "help": "Weight decay (L2 regularization) coefficient.",
        },
    )

    optimizer_type: Literal["adamw", "adam", "sgd", "adafactor"] = field(
        default="adamw",
        metadata={
            "help": f"Optimizer type. Options: 'adamw', 'adam', 'sgd', 'adafactor'",
        },
    )

    adam_beta1: float = field(
        default=0.9,
        metadata={
            "help": "Beta1 parameter for Adam optimizer.",
        },
    )

    adam_beta2: float = field(
        default=0.95,
        metadata={
            "help": "Beta2 parameter for Adam optimizer.",
        },
    )

    adam_epsilon: float = field(
        default=1e-8,
        metadata={
            "help": "Epsilon parameter for Adam optimizer.",
        },
    )

    max_grad_norm: float = field(
        default=1.0,
        metadata={
            "help": "Maximum gradient norm for gradient clipping. Set to 0 to disable.",
        },
    )

    scheduler_type: Literal[
        "cosine",
        "linear",
        "constant",
        "cosine_with_restarts",
        "polynomial",
        "constant_with_warmup",
    ] = field(
        default="cosine",
        metadata={
            "help": f"Learning rate scheduler type. Options: 'cosine', 'linear', 'constant', 'cosine_with_restarts', 'polynomial', 'constant_with_warmup'",
        },
    )

    warmup_steps: int = field(
        default=1000,
        metadata={
            "help": "Number of warmup steps for learning rate scheduler.",
        },
    )

    warmup_ratio: float = field(
        default=0.0,
        metadata={
            "help": (
                "Ratio of total steps for warmup (alternative to warmup_steps). "
                "If > 0, overrides warmup_steps."
            ),
        },
    )

    lr_scheduler_kwargs: Optional[dict] = field(
        default=None,
        metadata={
            "help": "Additional keyword arguments for learning rate scheduler.",
        },
    )

    use_amp: bool = field(
        default=True,
        metadata={
            "help": "Whether to use automatic mixed precision (AMP) training.",
        },
    )

    early_stopping_patience: int = field(
        default=3,
        metadata={
            "help": (
                "Number of evaluation steps with no improvement after which to stop training. "
                "Set to 0 to disable early stopping."
            ),
        },
    )

    save_steps: int = field(
        default=500,
        metadata={
            "help": "Save checkpoint every N steps.",
        },
    )

    eval_steps: Optional[int] = field(
        default=None,
        metadata={
            "help": (
                "Evaluate every N steps. "
                "If None, evaluates at the end of each epoch."
            ),
        },
    )

    logging_steps: int = field(
        default=100,
        metadata={
            "help": "Log training metrics every N steps.",
        },
    )

    seed: int = field(
        default=2021,
        metadata={
            "help": "Random seed for reproducibility.",
        },
    )

    local_rank: int = field(
        default=-1,
        metadata={
            "help": "Local rank for distributed training. Set by launcher.",
        },
    )

    world_size: int = field(
        default=1,
        metadata={
            "help": "Number of processes for distributed training.",
        },
    )

    ddp: bool = field(
        default=False,
        metadata={
            "help": "Whether to use Distributed Data Parallel (DDP).",
        },
    )

    gpu_ids: Optional[List[int]] = field(
        default=None,
        metadata={
            "help": "List of GPU IDs to use. If None, uses all available GPUs.",
        },
    )

    def __post_init__(self):
        if self.gradient_accumulation_steps < 1:
            raise ValueError("gradient_accumulation_steps must be >= 1")

        if self.learning_rate <= 0:
            raise ValueError("learning_rate must be positive")

        if isinstance(self.gpu_ids, str):
            self.gpu_ids = [int(x.strip()) for x in self.gpu_ids.split(",")]

        # Auto-detect world_size from available GPUs when DDP is enabled
        # but world_size is left at default (1).
        if self.ddp and self.world_size <= 1:
            import torch

            if self.gpu_ids:
                self.world_size = len(self.gpu_ids)
            elif torch.cuda.is_available():
                self.world_size = torch.cuda.device_count()
            else:
                self.world_size = 1

    @property
    def effective_batch_size(self) -> int:
        return (
            self.train_batch_size * self.gradient_accumulation_steps * self.world_size
        )

    @property
    def is_distributed(self) -> bool:
        return self.ddp and self.world_size > 1

    def to_dict(self) -> dict:
        return asdict(self)
