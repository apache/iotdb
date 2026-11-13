# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Copyright (c) NXAI GmbH.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from typing import Any, Literal

import torch
from torch import nn

from iotdb.ainode.core.model.tirex2.component.attention_block import AttentionBlock
from iotdb.ainode.core.model.tirex2.component.bi_xlstm import BiXLSTM


def _unwrap_output(
    output: torch.Tensor | tuple[torch.Tensor, Any],
) -> tuple[torch.Tensor, Any]:
    """Unwrap module output that may be a tensor or (tensor, state) tuple.

    Parameters
    ----------
    output : torch.Tensor or tuple[torch.Tensor, Any]
    Module output, either a single tensor or (tensor, state) tuple

    Returns
    -------
    tuple[torch.Tensor, Any]
    Tuple of (tensor, state). If input is a tensor, state is None.
    """
    if isinstance(output, tuple):
        return output
    return output, None


def variate_indices(
    group_vector, target_mask, known_covariate_mask
) -> tuple[torch.Tensor, torch.Tensor | None]:
    """Variates of the reverse xLSTM cell and of non-singleton attention groups.

    Computed once per forward pass, as selecting by mask synchronizes with the
    host, which would keep the block stack out of a CUDA graph.
    """
    reverse_idx = torch.where(
        ~target_mask if known_covariate_mask is None else known_covariate_mask
    )[0]
    attention_idx = None
    if group_vector is not None:
        _, inverse, counts = torch.unique(
            group_vector.flatten(), return_inverse=True, return_counts=True
        )
        attention_idx = torch.where(counts[inverse] > 1)[0]
    return reverse_idx, attention_idx


@dataclass
class TimeMixerConfig:
    """Configuration for time mixing model."""

    model_type: Literal["bi-slstm", "bi-mlstm"]

    act_fn: nn.Module

    embedding_dim: int
    num_heads: int = 4
    num_slstm_heads: int | None = None


@dataclass
class VariateMixerConfig:
    """Configuration for variate mixing model."""

    act_fn: nn.Module

    # dimensions
    embedding_dim: int
    num_heads: int = 4

    # whether to RMS-normalize query/key vectors in the attention layer
    use_qk_norm: bool = True

    # whether to use FlexAttention in the attention layer
    use_flex_attention: bool = False


@dataclass
class MultivariateBlockConfig:
    """Configuration for the MultivariateBlock.

    This block performs two-stage mixing:
        1. Time mixing: processes [B*V, L, P] - mixing across time dimension
        2. Variate mixing: processes [B*L, V, P] - mixing across variate dimension

    Normalization and residual connections are handled by the individual mixer components.
    """

    time_mixer: TimeMixerConfig
    variate_mixer: VariateMixerConfig
    block_order: list[str] | None = None

    # To initialize the xLSTM blocks
    block_idx: int | None = None
    num_blocks: int | None = None

    # Optional feed-forward network after variate mixing
    dropout: float = 0.1
    eps: float = 1e-6


class MultivariateBlock(nn.Module):
    """Two-stage mixing block for multivariate time series.

    Stage 1 (Time Mixing): Processes each variate's time series independently [B*V, L, P]
    Stage 2 (Variate Mixing): Reshapes to [L, B*V, P] and mixes across variates for each timestep.
    Stage 3 (FFN): Final feed-forward transformation on tokens.

    Parameters
    ----------
    config : MultivariateBlockConfig
    Configuration specifying time mixer, variate mixer, and FFN settings
    """

    def __init__(self, config: MultivariateBlockConfig) -> None:
        super().__init__()

        self.dropout = config.dropout if config.dropout is not None else 0.0

        self.eps = config.eps
        self.config = config

        # Build sub-blocks
        self.time_mixer = self._build_time_mixer(
            config.time_mixer,
            block_idx=config.block_idx,
            num_blocks=config.num_blocks,  # type: ignore
        )
        self.variate_mixer = self._build_variate_mixer(
            config.variate_mixer,
            block_idx=config.block_idx,
            num_blocks=config.num_blocks,  # type: ignore
        )

    def forward(
        self,
        x: torch.Tensor,
        group_vector: torch.Tensor | None = None,
        target_mask: torch.Tensor | None = None,
        known_covariate_mask: torch.Tensor | None = None,
        state: dict | None = None,
        *,
        indices: tuple | None = None,
        return_svd: bool = False,
    ) -> (
        tuple[torch.Tensor, dict[str, Any]]
        | tuple[torch.Tensor, dict[str, Any], dict[str, torch.Tensor]]
    ):
        """Forward pass with three-stage mixing.

        Normalization and residual connections are handled by the individual mixer components.

        Parameters
        ----------
        x : torch.Tensor
        Input tensor of shape [B*V, L, P] where:
            B = batch size
            V = number of variates
            L = sequence length
            P = feature dimension
            group_vector : torch.Tensor, optional
            Optional group ids used when attention is the variate mixer. Should broadcast to the
            variate dimension after reshaping (e.g., [B*V] or [B*V, 1])
            state : dict, optional
            Optional state returned from a previous call. Keys: ``time_mixer`` and ``variate_mixer``
            indices : tuple, optional
            :func:`variate_indices` of the masks, computed when omitted.
            return_svd : bool, optional
            Enables computation of the singular values on the first batch element in each sub-block.
            `torch.svd_lowrank` is used to approximately compute the singular values (default: False)

        Returns
        -------
        tuple[torch.Tensor, dict[str, Any]] or tuple[torch.Tensor, dict[str, Any], dict[str, torch.Tensor]]
        If return_svd is False, returns (tokens, state) where tokens has shape [B*V, L, P]
        and state is a dict with keys 'time_mixer' and 'variate_mixer'.
        If return_svd is True, returns (tokens, state, svd_vals) where svd_vals contains
        the singular values of the first batch element for each sub-block.
        """
        BV, L, P = x.shape
        reverse_idx, attention_idx = indices or variate_indices(
            group_vector, target_mask, known_covariate_mask
        )

        # Stage 1: Time Mixing [B*V, L, P] -> [B*V, L, P]
        # Each variate is processed independently across its time dimension
        time_output, _ = _unwrap_output(self.time_mixer(x, reverse_idx))

        # Stage 2: Variate Mixing
        # Transpose for variate mixing: [B*V, L, P] -> [L, B*V, P]
        # This treats L as the new batch dimension and mixes across B*V (all variates)
        x_variate = time_output.permute(1, 0, 2)  # [L, B*V, P]

        variate_output, _ = _unwrap_output(
            self.variate_mixer(
                x_variate,
                group_vector=group_vector,
                target_mask=target_mask,
                attention_idx=attention_idx,
            )
        )

        # Transpose back: [L, B*V, P] -> [B*V, L, P]
        x = variate_output.permute(1, 0, 2)

        return x, {}

    def _build_time_mixer(
        self, config: TimeMixerConfig, block_idx: int, num_blocks: int
    ) -> nn.Module:
        """Instantiate time mixing model from configuration.

        Parameters
        ----------
        config : TimeMixerConfig
        Configuration for the time mixer
        block_idx : int
        Index of this block in the overall stack
        num_blocks : int
        Total number of blocks in the stack

        Returns
        -------
        nn.Module
        Time mixing module (sLSTM, mLSTM, or attention)
        """
        match config.model_type:
            case "bi-slstm":
                cell_type, num_heads = (
                    "slstm",
                    config.num_slstm_heads or config.num_heads,
                )
            case "bi-mlstm":
                cell_type, num_heads = "mlstm", config.num_heads
            case _:
                raise ValueError(
                    f"Unknown model_type for time mixer '{config.model_type}'. Allowed 'bi-slstm' and 'bi-mlstm'"
                )

        return BiXLSTM(
            config.embedding_dim,
            num_heads,
            block_idx=block_idx,
            num_blocks=num_blocks,
            dropout=self.dropout,
            cell_type=cell_type,
        )

    def _build_variate_mixer(
        self, config: VariateMixerConfig, num_blocks: int, block_idx: int
    ) -> nn.Module:
        """Instantiate variate mixing model from configuration.

        Parameters
        ----------
        config : VariateMixerConfig
            Configuration for the variate mixer
        num_blocks : int
            Total number of blocks in the stack
        block_idx : int
            Index of this block in the overall stack

        Returns
        -------
        nn.Module
            Variate mixing module (attention, mLSTM, or identity)
        """
        model = AttentionBlock(
            input_dim=config.embedding_dim,
            n_heads=config.num_heads,
            act_fn=config.act_fn,
            dropout=self.dropout,
            use_qk_norm=config.use_qk_norm,
            use_flex_attention=config.use_flex_attention,
        )

        return model
