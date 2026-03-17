from dataclasses import asdict, dataclass, field
from typing import List, Literal, Optional, Union

SUPPORTED_FINETUNE_TYPES = ["full", "linear", "lora", "weaver_cnn", "weaver_mlp"]


@dataclass
class LoraArguments:
    lora_rank: int = field(
        default=2,
        metadata={
            "help": "The intrinsic dimension (rank) for LoRA. Default: r=2",
        },
    )

    lora_alpha: int = field(
        default=8,
        metadata={
            "help": (
                "The alpha parameter for LoRA scaling. "
                "Effective scaling = lora_alpha / lora_rank. Default: 8"
            ),
        },
    )

    lora_dropout: float = field(
        default=0.1,
        metadata={
            "help": "Dropout probability for LoRA layers. Default: 0.1",
        },
    )

    lora_target_modules: Optional[List[str]] = field(
        default_factory=lambda: ["q_proj", "v_proj"],
        metadata={
            "help": (
                "List of module names to apply LoRA. " "Default: ['q_proj', 'v_proj']"
            ),
        },
    )

    init_lora_weights: Union[Literal["gaussian", "loftq", "pissa", "olora"], bool] = (
        field(
            default="gaussian",
            metadata={
                "help": (
                    "LoRA weight initialization method. "
                    "'gaussian': Gaussian distribution init (default). "
                    "True: Default PEFT initialization. "
                    "False: Random initialization."
                ),
            },
        )
    )

    use_rslora: bool = field(
        default=False,
        metadata={
            "help": "Whether to use Rank-Stabilized LoRA (RSLoRA).",
        },
    )

    use_dora: bool = field(
        default=False,
        metadata={
            "help": "Whether to use Weight-Decomposed LoRA (DoRA).",
        },
    )

    def to_lora_config_dict(self) -> dict:
        """Convert to PEFT LoraConfig compatible dictionary."""
        return {
            "r": self.lora_rank,
            "lora_alpha": self.lora_alpha,
            "lora_dropout": self.lora_dropout,
            "target_modules": self.lora_target_modules,
            "init_lora_weights": self.init_lora_weights,
            "use_rslora": self.use_rslora,
            "use_dora": self.use_dora,
        }


@dataclass
class WeaverArguments:
    weaver_type: Literal["cnn", "mlp"] = field(
        default="cnn",
        metadata={
            "help": (
                "Feature Weaver architecture type. "
                "'cnn': CNN-based feature fusion (WeaverCNN). "
                "'mlp': MLP-based feature fusion (WeaverMLP)."
            ),
        },
    )

    input_channel: int = field(
        default=1,
        metadata={
            "help": "Number of input channels (covariates + target). Default: 1",
        },
    )

    output_channel: int = field(
        default=64,
        metadata={
            "help": "Hidden dimension for feature weaver. Default: 64",
        },
    )

    weaver_kernel_size: int = field(
        default=5,
        metadata={
            "help": "Kernel size for CNN-based weaver. Default: 5",
        },
    )

    weaver_dropout: float = field(
        default=0.1,
        metadata={
            "help": "Dropout probability in feature weaver. Default: 0.1",
        },
    )

    weaver_zero_init: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to use zero initialization for weaver weights. "
                "If True, all Conv1d and Linear weights are initialized to zero. "
                "If False (default), only the final fc layer is zero-initialized."
            ),
        },
    )

    test_pred_len: int = field(
        default=96,
        metadata={
            "help": "Prediction length for DualWeaver inference. Default: 96",
        },
    )

    test_n_sample: int = field(
        default=20,
        metadata={
            "help": "Number of samples for probabilistic prediction (Sundial). Default: 20",
        },
    )

    def to_weaver_config_dict(self) -> dict:
        """Convert to WeaverConfig compatible dictionary."""
        return {
            "weaver_type": self.weaver_type,
            "input_channel": self.input_channel,
            "output_channel": self.output_channel,
            "kernel_size": self.weaver_kernel_size,
            "dropout": self.weaver_dropout,
            "zero_init": self.weaver_zero_init,
        }


@dataclass
class FinetuneArguments(LoraArguments, WeaverArguments):
    """
    Arguments pertaining to fine-tuning method configuration.

    Example:
        >>> # Full fine-tuning
        >>> args = FinetuneArguments(finetune_type="full")
        >>>
        >>> # Linear probing (only train output head)
        >>> args = FinetuneArguments(finetune_type="linear")
        >>>
        >>> # LoRA fine-tuning
        >>> args = FinetuneArguments(
        ...     finetune_type="lora",
        ...     lora_rank=2,
        ...     lora_alpha=8,
        ... )
        >>>
        >>> # DualWeaver with CNN
        >>> args = FinetuneArguments(
        ...     finetune_type="weaver_cnn",
        ...     input_channel=16,
        ...     output_channel=64,
        ...     freeze_backbone=True,
        ... )
    """

    finetune_type: Literal["full", "linear", "lora", "weaver_cnn", "weaver_mlp"] = (
        field(
            default="full",
            metadata={
                "help": (
                    f"Type of adaptation/fine-tuning method. "
                    "'full': Update all model parameters. "
                    "'linear': Only update output head (linear probing). "
                    "'lora': Low-rank adaptation. "
                    "'weaver_cnn': CNN-based feature weaver for covariates (DualWeaver). "
                    "'weaver_mlp': MLP-based feature weaver for covariates (DualWeaver). "
                ),
            },
        )
    )

    priority: Literal["urgent", "normal", "low"] = field(
        default="normal",
        metadata={
            "help": "Task priority for fine-tuning queue.",
        },
    )

    freeze_backbone: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to freeze the backbone model during fine-tuning. "
                "If True, only adapter/head parameters are updated."
            ),
        },
    )

    trainable_modules: Optional[List[str]] = field(
        default=None,
        metadata={
            "help": (
                "List of module name patterns to make trainable. "
                "Supports wildcards, e.g., ['lm_head', 'flow_loss*']. "
                "If None, determined by finetune_type."
            ),
        },
    )

    freeze_modules: Optional[List[str]] = field(
        default=None,
        metadata={
            "help": (
                "List of module name patterns to explicitly freeze. "
                "Overrides trainable_modules for matching patterns."
            ),
        },
    )

    use_revin: bool = field(
        default=True,
        metadata={
            "help": (
                "Whether to use Reversible Instance Normalization (RevIN). "
                "RevIN improves generalization for time series forecasting."
            ),
        },
    )

    finetune_from_checkpoint: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Path to a fine-tuned checkpoint to continue training from. "
                "Different from base model path - this loads adapter weights too."
            ),
        },
    )

    merge_adapter_after_training: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to merge adapter weights into base model after training. "
                "Only applicable for LoRA and similar methods."
            ),
        },
    )

    def __post_init__(self):
        if self.finetune_type not in SUPPORTED_FINETUNE_TYPES:
            raise ValueError(
                f"Unsupported finetune_type: {self.finetune_type}. "
                f"Supported: {SUPPORTED_FINETUNE_TYPES}"
            )

        if isinstance(self.trainable_modules, str):
            self.trainable_modules = [
                m.strip() for m in self.trainable_modules.split(",")
            ]

        if isinstance(self.freeze_modules, str):
            self.freeze_modules = [m.strip() for m in self.freeze_modules.split(",")]

        if isinstance(self.lora_target_modules, str):
            self.lora_target_modules = [
                m.strip() for m in self.lora_target_modules.split(",")
            ]

    @property
    def is_full_finetune(self) -> bool:
        """Check if using full fine-tuning."""
        return self.finetune_type == "full"

    @property
    def is_adapter_based(self) -> bool:
        """Check if using adapter-based method."""
        return self.finetune_type in ["lora", "weaver_cnn", "weaver_mlp"]

    @property
    def uses_weaver(self) -> bool:
        """Check if using DualWeaver."""
        return self.finetune_type in ["weaver_cnn", "weaver_mlp"]

    @property
    def uses_lora(self) -> bool:
        """Check if using LoRA."""
        return self.finetune_type == "lora"

    def get_trainable_module_patterns(self) -> List[str]:
        """
        Get list of module patterns that should be trainable.

        Returns:
            List of module name patterns
        """
        if self.trainable_modules is not None:
            return self.trainable_modules

        patterns = {
            "full": ["*"],
            "linear": ["lm_head", "flow_loss", "output_patch_embedding"],
            "lora": ["lora_*"],
            "weaver_cnn": ["feature_weaver*", "a", "b"],
            "weaver_mlp": ["feature_weaver*", "a", "b"],
        }
        return patterns.get(self.finetune_type, ["*"])

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
