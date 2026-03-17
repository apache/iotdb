import torch
import torch.nn as nn

from standalone_finetune.finetune.adapter.weaver.base_weaver import (
    BaseFeatureWeaver,
    WeaverConfig,
)


class WeaverMLP(BaseFeatureWeaver):
    """
    MLP-based Feature Weaver, strictly following electricity-price-forecasting implementation.

    Network structure:
        Linear(input_channel -> output_channel) -> SiLU -> Dropout
        Linear(output_channel -> input_channel)  # zero-initialized by default

    Features:
        1. Simple two-layer MLP structure
        2. fc2 layer is zero-initialized by default (ensures output is near zero at training start)
        3. Uses SiLU activation function
        4. Uses standard nn.Dropout, controlled by model.train()/eval()
    """

    def __init__(self, config: WeaverConfig):
        super().__init__(config)

        input_channel = config.input_channel
        output_channel = config.output_channel
        dropout = config.dropout

        self.fc1 = nn.Linear(input_channel, output_channel)
        self.fc2 = nn.Linear(output_channel, input_channel)
        self.silu = nn.SiLU()
        self.dropout = nn.Dropout(p=dropout)

        # Apply zero initialization
        if config.zero_init:
            self._apply_zero_init_to_all()
        else:
            self._apply_zero_init(self.fc2)

    def forward(self, x: torch.Tensor, is_output_seq: bool = False) -> torch.Tensor:
        """
        Forward pass.

        Args:
            x: Input tensor [B, L, C]
            is_output_seq: Ignored (for API compatibility with WeaverCNN).
                           MLP has no LayerNorm, so sequence length doesn't matter.

        Returns:
            Transformed features [B, L, C]
        """
        x = self.fc1(x)
        x = self.silu(x)
        x = self.dropout(x)
        x = self.fc2(x)

        return x
