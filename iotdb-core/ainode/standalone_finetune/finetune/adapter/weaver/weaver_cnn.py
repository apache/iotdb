import torch
import torch.nn as nn

from standalone_finetune.finetune.adapter.weaver.base_weaver import (
    BaseFeatureWeaver,
    WeaverConfig,
)


class WeaverCNN(BaseFeatureWeaver):
    """
    CNN-based Feature Weaver, strictly following electricity-price-forecasting implementation.

    Network structure:
        Conv1d(input_channel -> output_channel) -> LayerNorm -> SiLU -> Dropout
        Conv1d(output_channel -> input_channel) -> LayerNorm -> SiLU -> Dropout
        Linear(input_channel -> input_channel)

    Features:
        1. Supports type=0/1 to switch between different LayerNorms
           (for handling different input/output sequence lengths)
        2. Uses replicate padding to preserve sequence length
        3. Supports zero initialization for training stability
        4. Uses SiLU activation function
    """

    def __init__(self, config: WeaverConfig):
        super().__init__(config)

        input_channel = config.input_channel
        output_channel = config.output_channel
        seq_len = config.seq_len
        input_token_len = config.input_token_len
        max_output_token_len = config.max_output_token_len
        kernel_size = config.kernel_size
        dropout = config.dropout
        padding = kernel_size // 2

        # Compute output sequence length
        output_seq_len = seq_len - input_token_len + max_output_token_len

        if input_token_len == max_output_token_len:
            # Simplified version: use Sequential with fixed LayerNorm
            self.conv1 = nn.Sequential(
                nn.Conv1d(
                    input_channel,
                    output_channel,
                    kernel_size,
                    stride=1,
                    padding=padding,
                    dilation=1,
                    padding_mode="replicate",
                ),
                nn.LayerNorm([output_channel, seq_len]),
                nn.SiLU(),
                nn.Dropout1d(p=dropout),
            )
            self.conv2 = nn.Sequential(
                nn.Conv1d(
                    output_channel,
                    input_channel,
                    kernel_size,
                    stride=1,
                    padding=padding,
                    dilation=1,
                    padding_mode="replicate",
                ),
                nn.LayerNorm([input_channel, seq_len]),
                nn.SiLU(),
                nn.Dropout1d(p=dropout),
            )
            self._use_dual_norm = False
        else:
            # Full version: need separate LayerNorms for input/output sequences
            self.conv1 = nn.Conv1d(
                input_channel,
                output_channel,
                kernel_size,
                stride=1,
                padding=padding,
                dilation=1,
                padding_mode="replicate",
            )
            self.layernorm_1_x = nn.LayerNorm([output_channel, seq_len])
            self.layernorm_1_y = nn.LayerNorm([output_channel, output_seq_len])

            self.conv2 = nn.Conv1d(
                output_channel,
                input_channel,
                kernel_size,
                stride=1,
                padding=padding,
                dilation=1,
                padding_mode="replicate",
            )
            self.layernorm_2_x = nn.LayerNorm([input_channel, seq_len])
            self.layernorm_2_y = nn.LayerNorm([input_channel, output_seq_len])

            self.silu = nn.SiLU()
            self.dropout = nn.Dropout1d(p=dropout)
            self._use_dual_norm = True

        self.fc = nn.Linear(input_channel, input_channel)

        # Apply zero initialization
        if config.zero_init:
            self._apply_zero_init_to_all()
        else:
            # Only zero-init the final fc layer (default behavior from original code)
            self._apply_zero_init(self.fc)

    def forward(self, x: torch.Tensor, is_output_seq: bool = False) -> torch.Tensor:
        """
        Forward pass.

        Args:
            x: Input tensor [B, L, C]
            is_output_seq: Whether processing output/label sequence (batch_y).
                  False = input sequence batch_x (length = seq_len)
                  True  = output sequence batch_y (length = output_seq_len)

        Returns:
            Transformed features [B, L, C]
        """
        B, L, C = x.shape
        # [B, L, C] -> [B, C, L]
        x = x.permute(0, 2, 1)

        if not self._use_dual_norm:
            # Simplified path: Sequential handles everything
            x = self.conv1(x)
            x = self.conv2(x)
        else:
            # Full path: manually apply LayerNorm based on sequence type
            x = self.conv1(x)
            if is_output_seq:
                x = self.layernorm_1_y(x)
            else:
                x = self.layernorm_1_x(x)
            x = self.silu(x)
            x = self.dropout(x)

            x = self.conv2(x)
            if is_output_seq:
                x = self.layernorm_2_y(x)
            else:
                x = self.layernorm_2_x(x)
            x = self.silu(x)
            x = self.dropout(x)

        # [B, C, L] -> [B, L, C]
        x = x.permute(0, 2, 1)
        x = self.fc(x)

        return x
