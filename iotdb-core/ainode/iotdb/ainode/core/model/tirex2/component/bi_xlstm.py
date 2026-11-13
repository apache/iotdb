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

"""This module exposes xLSTM blocks combining recurrent kernels with feed-forward adapters."""

from typing import Literal

import torch
from torch import nn

from iotdb.ainode.core.model.tirex2.component.feedforward import FeedForward
from iotdb.ainode.core.model.tirex2.component.mlstm_block import (
    mLSTMLayer,
    mLSTMLayerConfig,
)
from iotdb.ainode.core.model.tirex2.component.norm import RMSNorm
from iotdb.ainode.core.model.tirex2.component.slstm_block import (
    sLSTMLayer,
    sLSTMLayerConfig,
)


class BiXLSTM(nn.Module):
    """Bi-directional xLSTM block that splits variates into forward and reverse subsets,
    runs a separate xLSTM cell over each, and recombines the results.

    The cell type is controlled by ``cell_type``:
      - ``"slstm"``: Uses sLSTM cells (original behaviour, equivalent to the former BiSLSTM).
      - ``"mlstm"``: Uses mLSTM cells.

    The split is controlled by ``split_method``:
      - ``"reverse_wo_target"``: The forward cell sees all variates, while the
        reverse cell sees only the non-target covariates. The non-target variates
        thus appear in both directions and are shared.
      - ``"forward_only_target"``: The forward cell sees only the target variates
        and the reverse cell sees only the non-target covariates. No variates are
        shared between the two directions.
      - ``"reverse_known_only"``: The forward cell sees all variates, while the
        reverse cell sees only the known covariates (those with non-NaN values in
        the future window). Past-only covariates are processed forward-only.

    Variates that appear in both directions have their forward and reverse
    representations concatenated along the embedding dimension (2*D) and
    downprojected back to D via a linear layer with dropout. Variates that appear
    in only one direction keep their single-direction output unchanged. Residual
    connections wrap both the recurrent stage and the subsequent feed-forward block.

    Parameters
    ----------
    embedding_dim : int
        Model dimension.
    num_heads : int
        Number of heads of the recurrent cell.
    block_idx : int
        Index of this block in the overall stack.
    num_blocks : int
        Total number of blocks in the stack.
    dropout : float
        Dropout probability applied in the combination projection.
    split_method : {"forward_only_target", "reverse_wo_target", "reverse_known_only"}
        Strategy for splitting variates between forward and reverse cells.
    share_weights : bool
        When True, the forward and reverse cells share the same parameters.
    cell_type : {"slstm", "mlstm"}
        Which recurrent cell to instantiate.
    """

    def __init__(
        self,
        embedding_dim: int,
        num_heads: int,
        block_idx: int,
        num_blocks: int,
        dropout: float = 0.0,
        share_weights: bool = True,
        cell_type: Literal["slstm", "mlstm"] = "slstm",
    ):
        super().__init__()
        self.cell_type = cell_type
        self.norm_lstm = RMSNorm(embedding_dim, eps=1e-6)
        self._split = self._split_variates
        self._recombine = self._recombine_variates

        match cell_type:
            case "slstm":
                slstm_config = sLSTMLayerConfig(
                    embedding_dim=embedding_dim,
                    num_heads=num_heads,
                    recurrent_weight_init="zeros",
                    bias_init="powerlaw_blockdependent",
                )
                self.fwd_cell = sLSTMLayer(
                    slstm_config, block_idx=block_idx, num_blocks=num_blocks
                )
            case "mlstm":
                mlstm_config = mLSTMLayerConfig(
                    embedding_dim=embedding_dim,
                    num_heads=num_heads,
                )
                self.fwd_cell = mLSTMLayer(mlstm_config)
            case _:
                raise ValueError(
                    f"Unknown cell type '({cell_type})'. Allowed 'mlstm' or 'slstm'."
                )

        self.combination = nn.Linear(2 * embedding_dim, embedding_dim, bias=False)
        self.combination_dropout = nn.Dropout(dropout)

        self.norm_ffn = RMSNorm(embedding_dim, eps=1e-6)
        self.ffn = FeedForward(embedding_dim)

    @property
    def rev_cell(self) -> nn.Module:
        """The reverse direction shares the forward cell; it is registered (and stored) once."""
        return self.fwd_cell

    def _load_from_state_dict(self, state_dict, prefix, *args, **kwargs):
        # Older checkpoints also store the shared cell under ``rev_cell``.
        for key in [key for key in state_dict if key.startswith(f"{prefix}rev_cell.")]:
            del state_dict[key]
        super()._load_from_state_dict(state_dict, prefix, *args, **kwargs)

    def forward(self, x: torch.Tensor, reverse_idx: torch.Tensor):
        """Run the bidirectional xLSTM block; ``reverse_idx`` are the variates of the reverse cell."""
        x_norm = self.norm_lstm(x)

        x_fwd, x_rev, fwd_idc, rev_idc = self._split(x_norm, reverse_idx)

        x_fwd = self.fwd_cell(x_fwd) if x_fwd.shape[0] > 0 else x_fwd
        if x_rev.shape[0] > 0:
            x_rev_flipped = torch.flip(x_rev, dims=(1,)).contiguous()
            x_rev_out = self.rev_cell(x_rev_flipped)
            x_rev = torch.flip(x_rev_out, dims=(1,))

        out = self._recombine(x_fwd, x_rev, fwd_idc, rev_idc, x)

        # residual connection around lstm + combination
        out = x + out

        # ffn with residual connection
        out = out + self.ffn(self.norm_ffn(out))

        return out

    def _split_variates(
        self, x: torch.Tensor, known_idc: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """Forward cell sees all variates, reverse sees only known covariates (non-NaN future)."""
        return x, x[known_idc], torch.arange(x.shape[0], device=x.device), known_idc

    def _recombine_variates(
        self,
        x_fwd: torch.Tensor,
        x_rev: torch.Tensor,
        fwd_idc: torch.Tensor,
        rev_idc: torch.Tensor,
        ref: torch.Tensor,
    ) -> torch.Tensor:
        """Covariates appear in both directions and are combined; targets are fwd-only."""
        # combine covariate representations from both directions
        x_shared = torch.cat([x_fwd[rev_idc], x_rev], dim=-1)
        x_shared = self.combination_dropout(self.combination(x_shared))

        # start from fwd output (all variates), overwrite covariates with combined
        out = x_fwd.to(ref.dtype)
        out[rev_idc] = x_shared.to(ref.dtype)
        return out
