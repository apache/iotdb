from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal

import torch
import torch.nn as nn
import torch.nn.init as init


@dataclass
class WeaverConfig:
    """
    Configuration class for Feature Weaver.

    Attributes:
        weaver_type: Type of weaver ("cnn" or "mlp")
        input_channel: Number of input channels (covariates + target)
        output_channel: Hidden dimension
        seq_len: Input sequence length
        input_token_len: Input token length
        max_output_token_len: Maximum output token length
        kernel_size: Kernel size for CNN (default: 5)
        dropout: Dropout probability (default: 0.1)
        zero_init: Whether to use zero initialization (default: False)
    """

    weaver_type: Literal["cnn", "mlp"] = "cnn"
    input_channel: int = 10
    output_channel: int = 64
    seq_len: int = 720
    input_token_len: int = 96
    max_output_token_len: int = 96
    kernel_size: int = 5
    dropout: float = 0.1
    zero_init: bool = False

    def __post_init__(self):
        # Compute output sequence length (for type=1 LayerNorm in CNN)
        self.output_seq_len = (
            self.seq_len - self.input_token_len + self.max_output_token_len
        )

    @classmethod
    def from_dict(cls, config_dict: dict) -> "WeaverConfig":
        """Create config from dictionary."""
        return cls(
            **{k: v for k, v in config_dict.items() if k in cls.__dataclass_fields__}
        )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "weaver_type": self.weaver_type,
            "input_channel": self.input_channel,
            "output_channel": self.output_channel,
            "seq_len": self.seq_len,
            "input_token_len": self.input_token_len,
            "max_output_token_len": self.max_output_token_len,
            "kernel_size": self.kernel_size,
            "dropout": self.dropout,
            "zero_init": self.zero_init,
        }


class BaseFeatureWeaver(nn.Module, ABC):
    """
    Abstract base class for Feature Weaver.

    Feature Weaver learns the relationship between covariates and target variable,
    generating enhanced feature representations for DualWeaver's dual-path transformation.
    """

    def __init__(self, config: WeaverConfig):
        super().__init__()
        self.config = config

    @abstractmethod
    def forward(self, x: torch.Tensor, is_output_seq: bool = False) -> torch.Tensor:
        """
        Forward pass.

        Args:
            x: Input tensor [B, T, C]
            is_output_seq: Whether the input is an output sequence (batch_y).
                  False = input sequence batch_x (length = seq_len)
                  True  = output sequence batch_y (length = seq_len - input_token_len + max_output_token_len)

        Returns:
            Transformed features [B, T, C]
        """
        pass

    @staticmethod
    def _apply_zero_init(layer: nn.Module) -> None:
        """Apply zero initialization to a layer."""
        init.zeros_(layer.weight)
        if layer.bias is not None:
            init.zeros_(layer.bias)

    def _apply_zero_init_to_all(self) -> None:
        """Apply zero initialization to all Conv1d and Linear layers."""
        for m in self.modules():
            if isinstance(m, (nn.Conv1d, nn.Linear)):
                self._apply_zero_init(m)


def get_weaver(config: WeaverConfig) -> BaseFeatureWeaver:
    """
    Factory function to create Feature Weaver based on config.

    Args:
        config: WeaverConfig object

    Returns:
        WeaverCNN or WeaverMLP instance
    """
    from standalone_finetune.finetune.adapter.weaver.weaver_cnn import WeaverCNN
    from standalone_finetune.finetune.adapter.weaver.weaver_mlp import WeaverMLP

    if config.weaver_type == "cnn":
        return WeaverCNN(config)
    elif config.weaver_type == "mlp":
        return WeaverMLP(config)
    else:
        raise ValueError(
            f"Unknown weaver_type: {config.weaver_type}. Use 'cnn' or 'mlp'."
        )
