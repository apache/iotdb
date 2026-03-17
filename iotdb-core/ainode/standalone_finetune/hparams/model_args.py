from dataclasses import asdict, dataclass, field
from typing import List, Literal

SUPPORTED_MODELS = ["sundial", "chronos2", "timer", "timer_xl", "moirai2"]


@dataclass
class ModelArguments:
    """
    Arguments pertaining to which model to use and how to configure it.
    """

    model_type: Literal["sundial", "chronos2", "timer", "timer_xl", "moirai2"] = field(
        default="sundial",
        metadata={
            "help": f"Type of time series foundation model. Options: {SUPPORTED_MODELS}",
            "choices": SUPPORTED_MODELS,
        },
    )

    base_model_path: str = field(
        default="",
        metadata={
            "help": (
                "Path to pretrained model checkpoint directory or HuggingFace model identifier. "
                "For local models, provide the directory containing config.json and model weights."
            ),
        },
    )

    model_id: str = field(
        default="finetuned_model",
        metadata={
            "help": "Unique identifier for the fine-tuned model output.",
        },
    )

    base_model_id: str = field(
        default="",
        metadata={
            "help": "Unique identifier for the base model.",
        },
    )

    input_token_len: int = field(
        default=16,
        metadata={
            "help": "Number of time series data points per input token.",
        },
    )

    output_token_lens: List[int] = field(
        default_factory=lambda: [96],
        metadata={
            "help": (
                "List of prediction horizons (output token lengths). "
                "Matches the model config format used by Sundial / Timer XL."
            ),
        },
    )

    seq_len: int = field(
        default=2880,
        metadata={
            "help": "Total sequence length (lookback window) for model input.",
        },
    )

    n_samples: int = field(
        default=10,
        metadata={
            "help": (
                "Number of samples for probabilistic forecasting (Sundial). "
                "Final prediction is the mean of n_samples."
            ),
        },
    )

    trust_remote_code: bool = field(
        default=True,
        metadata={
            "help": "Whether to trust and execute remote code when loading models.",
        },
    )

    torch_dtype: Literal["auto", "float32", "float16", "bfloat16"] = field(
        default="float32",
        metadata={
            "help": (
                "Data type for model weights. "
                "'auto' will use the dtype from model config."
            ),
        },
    )

    low_cpu_mem_usage: bool = field(
        default=False,
        metadata={
            "help": (
                "Whether to use low CPU memory mode when loading model. "
                "Requires accelerate library."
            ),
        },
    )

    def __post_init__(self):
        # Coerce scalar → list for robustness (e.g. YAML has `output_token_lens: 96`)
        if isinstance(self.output_token_lens, (int, float)):
            self.output_token_lens = [int(self.output_token_lens)]

        if self.model_type not in SUPPORTED_MODELS:
            raise ValueError(
                f"Unsupported model_type: {self.model_type}. "
                f"Supported: {SUPPORTED_MODELS}"
            )

    @property
    def output_token_len(self) -> int:
        """Largest prediction horizon (convenience accessor)."""
        return max(self.output_token_lens)

    def get_torch_dtype(self):
        import torch

        dtype_map = {
            "float32": torch.float32,
            "float16": torch.float16,
            "bfloat16": torch.bfloat16,
            "auto": "auto",
        }
        return dtype_map.get(self.torch_dtype, torch.float32)

    def to_dict(self) -> dict:
        return asdict(self)
