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

import math
import warnings
from typing import Any

import torch
from torch.nn import functional as F

from iotdb.ainode.core.model.tirex2.component.feedforward import MLP

FLEX_MASK_BLOCK_SIZE = 128
FLEX_KERNEL_BLOCK_SIZE = 32

create_block_mask = None
_flex_attention = None
flex_attention = None
_flex_attention_import_attempted = False


def _load_flex_attention() -> bool:
    """Load FlexAttention on demand, only for layers that opt into it."""
    global create_block_mask
    global _flex_attention
    global flex_attention
    global _flex_attention_import_attempted

    if flex_attention is not None:
        return True
    if _flex_attention_import_attempted:
        return False

    _flex_attention_import_attempted = True
    try:
        from torch.nn.attention.flex_attention import (
            create_block_mask as imported_create_block_mask,
        )
        from torch.nn.attention.flex_attention import (
            flex_attention as imported_flex_attention,
        )
    except ImportError:  # pragma: no cover - depends on installed PyTorch version
        return False

    create_block_mask = imported_create_block_mask
    _flex_attention = imported_flex_attention
    # FlexAttention is intended to run through the compiled fused kernel.
    # dynamic=True avoids recompiling for every distinct sequence length.
    flex_attention = torch.compile(imported_flex_attention, dynamic=True)
    return True


def is_flex_attention_available() -> bool:
    """Return whether FlexAttention can be loaded in the current PyTorch install."""
    return _load_flex_attention()


def _require_flex_attention() -> None:
    if not _load_flex_attention():
        raise RuntimeError(
            "FlexAttention was requested, but torch.nn.attention.flex_attention "
            "is not available in this PyTorch installation."
        )


def norm(x: torch.Tensor) -> torch.Tensor:
    """Apply RMS normalization over last dimension."""
    return F.rms_norm(x, (x.size(-1),))


class AttentionLayer(torch.nn.Module):
    """
    Multi-head attention with an optional group-wise mask.

    Input is expected as `[B, L, D]`, where attention is computed across the length dimension `L` (this may
    represent time or variates depending on the caller). When a group mask is provided, attention scores
    between tokens with different group ids are masked out to form a block-diagonal attention pattern.
    Key/query/value vectors are assumed to be pre-normalized and any residual connections or output
    normalization should be handled by the caller.

    Parameters
    ----------
    input_dim: int
        Dimension of the incoming key/query/value vectors.
    kv_proj_dim: int
        Dimension of each attention head after projection.
    n_heads: int
        Number of attention heads.
    dropout: float
        Dropout probability used inside scaled dot-product attention during training.
    use_qk_norm: bool
        Whether to apply RMS normalization to the query and key vectors before
        computing attention scores (default: True).
    use_flex_attention: bool
        Whether to use FlexAttention instead of scaled dot product attention
        (default: False).
    """

    def __init__(
        self,
        input_dim: int,
        kv_proj_dim: int,
        n_heads: int = 4,
        dropout: float = 0,
        disable_singleton_attention: bool = False,
        use_rope: bool = False,
        no_qk_scale: bool = False,
        use_qk_norm: bool = True,
        use_flex_attention: bool = False,
    ):
        super().__init__()
        self.input_dim: int = input_dim
        self.kv_proj_dim: int = kv_proj_dim
        self.n_heads: int = n_heads
        self.dropout: float = dropout
        self.disable_singleton_attention: bool = disable_singleton_attention
        self.use_rope: bool = use_rope
        self.use_qk_norm: bool = use_qk_norm
        self.use_flex_attention: bool = use_flex_attention
        if self.use_flex_attention:
            _require_flex_attention()

        self.embedding_dim = kv_proj_dim * n_heads

        self.WK = self.create_weights(self.input_dim, self.embedding_dim, bias=False)
        self.WQ = self.create_weights(self.input_dim, self.embedding_dim, bias=False)
        self.WV = self.create_weights(self.input_dim, self.embedding_dim, bias=False)
        self.WO = self.create_weights(self.embedding_dim, self.input_dim, bias=False)

        self.scale = 1 / math.sqrt(self.kv_proj_dim) if not no_qk_scale else 1

    def create_weights(self, input_dim, embedding_dim, bias: bool) -> torch.nn.Linear:
        """Create and initialize a linear projection layer."""
        linear = torch.nn.Linear(
            in_features=input_dim, out_features=embedding_dim, bias=bias
        )
        return linear

    def _build_group_block_mask(
        self,
        group_vector: torch.Tensor,
        target_mask: torch.Tensor,
        seq_len: int,
    ):
        """Build the block-sparse FlexAttention mask matching the dense group mask."""
        _require_flex_attention()
        assert create_block_mask is not None

        group_vector = (
            group_vector.squeeze(-1) if group_vector.ndim == 2 else group_vector
        )
        target_mask = target_mask.squeeze(-1) if target_mask.ndim == 2 else target_mask
        target_mask = target_mask.to(torch.bool)

        def mask_mod(b, h, q_idx, kv_idx):
            same_group = group_vector[q_idx] == group_vector[kv_idx]
            covariate_to_target = (~target_mask[q_idx]) & target_mask[kv_idx]
            return same_group & ~covariate_to_target

        return create_block_mask(
            mask_mod,
            B=None,
            H=None,
            Q_LEN=seq_len,
            KV_LEN=seq_len,
            device=group_vector.device,
            BLOCK_SIZE=FLEX_MASK_BLOCK_SIZE,
        )

    def forward(
        self,
        x: torch.Tensor,
        group_vector: torch.Tensor | None = None,
        target_mask: torch.Tensor | None = None,
        state: Any | None = None,
    ) -> torch.Tensor:
        """
        Compute attention with an optional group mask.

        Inputs are expected to be pre-normalized. The output contains only the attention transformation; any
        residual connection must be applied by the caller if needed.

        Parameters
        ----------
        x: torch.Tensor
            Pre-normalized input tensor of shape [B, L, D].
        group_vector: torch.Tensor | None
            Optional group ids with shape `[L]` or `[L, 1]` aligned to the sequence dimension. Positions
            with different ids are masked from attending to each other. If `None`, attention is computed
            across all positions.
        state: Any | None
            Unused placeholder for compatibility with other mixer interfaces.

        Returns
        -------
        torch.Tensor
            The attention-transformed tokens of shape `[B, L, D]`.
        """
        _, L, _ = x.shape

        def unpack(x):
            """Reshape from (B, L, D) to multi-head format (B, H, L, D_head)."""
            return x.reshape(x.shape[0], L, self.n_heads, self.kv_proj_dim).permute(
                0, 2, 1, 3
            )

        def pack(x):
            """Reshape from multi-head format (B, H, L, D_head) to (B, L, D)."""
            return x.permute(0, 2, 1, 3).reshape(
                x.shape[0], L, self.n_heads * self.kv_proj_dim
            )

        qk_norm = norm if self.use_qk_norm else (lambda t: t)
        Q = qk_norm(
            unpack(self.WQ(x))
        )  # [B, L, D_in] -> [B, L, D_embed] -> [B, H, L, D_head]
        K = qk_norm(
            unpack(self.WK(x))
        )  # [B, L, D_in] -> [B, L, D_embed] -> [B, H, L, D_head]
        V = unpack(self.WV(x))  # [B, L, D_in] -> [B, L, D_embed] -> [B, H, L, D_head]

        mask = None
        block_mask = None
        # create mask, allow attention only between elements with the same group id
        if group_vector is not None:
            if group_vector.shape[0] != L:
                raise RuntimeError(
                    f"Group mask length has to be the same as first dimension of x, "
                    f"but found {group_vector.shape[0]} and {L}, respectively"
                )

            group_vector = (
                group_vector.squeeze(-1) if group_vector.ndim == 2 else group_vector
            )

            if target_mask is None:
                raise RuntimeError(
                    f"Attention shall restrict information flow between target and covariates but no target_mask provided"
                )

            if self.use_flex_attention:
                block_mask = self._build_group_block_mask(group_vector, target_mask, L)
            else:
                mask = group_vector.unsqueeze(0) == group_vector.unsqueeze(1)  # [L, L]
                target_mask = (
                    target_mask.squeeze(-1) if target_mask.ndim == 2 else target_mask
                )
                # Block covariates (rows where ~target_mask) from attending to targets (cols where target_mask)
                covariate_to_target = ~target_mask.unsqueeze(1) & target_mask.unsqueeze(
                    0
                )  # [L, L]
                mask = mask & ~covariate_to_target
                mask = mask[
                    None, None, ...
                ]  # mask has to be broadcastable to query dimensions

        # Compute attention
        if self.use_flex_attention:
            _require_flex_attention()
            if Q.is_cuda:
                assert flex_attention is not None
                attention_out = flex_attention(  # [B, H, L, D_head]
                    Q,
                    K,
                    V,
                    block_mask=block_mask,
                    scale=self.scale,
                    kernel_options={
                        "BLOCK_M": FLEX_KERNEL_BLOCK_SIZE,
                        "BLOCK_N": FLEX_KERNEL_BLOCK_SIZE,
                    },
                )
            else:
                assert _flex_attention is not None
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message="flex_attention called without torch.compile",
                        category=UserWarning,
                    )
                    attention_out = _flex_attention(  # [B, H, L, D_head]
                        Q,
                        K,
                        V,
                        block_mask=block_mask,
                        scale=self.scale,
                    )
        else:
            attention_out = (
                torch.nn.functional.scaled_dot_product_attention(  # [B, H, L, D_head]
                    Q,
                    K,
                    V,
                    attn_mask=mask,
                    dropout_p=self.dropout if self.training else 0.0,
                    scale=self.scale,
                )
            )

        packed_attention = pack(attention_out)  # [B, H, L, D_head] -> [B, L, D_embed]
        x_trans = self.WO(packed_attention)  # [B, L, D_embed] -> [B, L, D_in]
        x_trans = torch.nn.functional.dropout(
            x_trans, self.dropout, training=self.training
        )

        return x_trans


class AttentionBlock(torch.nn.Module):
    """Transformer attention block with RMS-normalized attention and FFN residuals.

    The block combines multi-head attention with a feed-forward network. Optional
    group-wise attention masks are applied when group IDs are supplied at forward time.
    """

    def __init__(
        self,
        input_dim: int,
        n_heads: int,
        dropout: float,
        act_fn: torch.nn.Module,
        use_qk_norm: bool = True,
        use_flex_attention: bool = False,
    ):
        super().__init__()
        self.input_dim = input_dim
        self.n_heads = n_heads
        self.dropout = dropout
        self.use_qk_norm = use_qk_norm
        self.use_flex_attention = use_flex_attention
        self._collect_group_stats_this_step = False
        self.use_post_norm = False

        assert (
            input_dim % n_heads == 0
        ), f"Input dimension {input_dim} has do be divisible by the number of heads {n_heads}"
        self.kv_proj_dim = input_dim // n_heads

        self.norm_attn = torch.nn.RMSNorm(self.input_dim, eps=1e-6)
        self.attn = AttentionLayer(
            input_dim=input_dim,
            kv_proj_dim=self.kv_proj_dim,
            n_heads=self.n_heads,
            dropout=self.dropout,
            use_qk_norm=self.use_qk_norm,
            use_flex_attention=self.use_flex_attention,
        )

        self.norm_ffn = torch.nn.RMSNorm(self.input_dim, eps=1e-6)
        self.ffn = MLP(self.input_dim, 4 * self.input_dim, act_fn, dropout=self.dropout)

    def forward(
        self,
        x: torch.Tensor,
        *args,
        group_vector: torch.Tensor | None = None,
        target_mask: torch.Tensor | None = None,
        attention_idx: torch.Tensor | None = None,
        **kwargs,
    ) -> torch.Tensor:
        """Forward pass through attention and FFN with residual connections.

        Parameters
        ----------
        x : torch.Tensor
            Input tensor of shape [B, L, D]
        group_vector : torch.Tensor or None, optional
            Optional group ids for group-wise attention masking
        attention_idx : torch.Tensor or None, optional
            Variates in non-singleton groups (see ``variate_indices``), derived when omitted.
        *args
            Unused positional arguments for interface compatibility
        **kwargs
            Unused keyword arguments for interface compatibility

        Returns
        -------
        torch.Tensor
            Output tensor of shape [B, L, D].
        """
        x_in = x

        if group_vector is not None:
            if attention_idx is None:
                gv = (
                    group_vector.squeeze(-1) if group_vector.ndim == 2 else group_vector
                )
                _, inverse, counts = torch.unique(
                    gv, return_inverse=True, return_counts=True
                )
                attention_idx = torch.where(counts[inverse] > 1)[0]
            if attention_idx.numel() == 0:
                return x_in  # batch is entirely singleton groups -> block is a no-op
            x = x[:, attention_idx, :]
            group_vector = group_vector[attention_idx]
            if target_mask is not None:
                target_mask = target_mask[attention_idx]

            assert (
                x.shape[1] == group_vector.shape[0]
            ), f"gather shape mismatch: x={x.shape[1]}, gv={group_vector.shape[0]}"
            if target_mask is not None:
                assert (
                    target_mask.shape[0] == x.shape[1]
                ), f"target_mask not sliced consistently: tm={target_mask.shape[0]}, x={x.shape[1]}"

        x_attn = self.attn(
            self.norm_attn(x), group_vector=group_vector, target_mask=target_mask
        )
        x = x + x_attn

        x_ffn = self.ffn(self.norm_ffn(x))
        x = x + x_ffn
        # Scatter non-singleton outputs back into the full-L tensor
        if group_vector is not None:
            out = x_in.clone()
            out[:, attention_idx, :] = x
            assert out.shape == x_in.shape
            x = out

        return x
