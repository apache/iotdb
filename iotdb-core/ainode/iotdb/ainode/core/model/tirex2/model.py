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

import logging
from dataclasses import dataclass, replace
from typing import Any, Literal

import torch
from torch import nn

from iotdb.ainode.core.model.tirex2.component.norm import RMSNorm
from iotdb.ainode.core.model.tirex2.component.patch_tokenizer import Tokenizer
from iotdb.ainode.core.model.tirex2.component.postprocessor import PostProcessor
from iotdb.ainode.core.model.tirex2.component.residual_block import ResidualBlock
from iotdb.ainode.core.model.tirex2.component.scaler import Scaler
from iotdb.ainode.core.model.tirex2.component.variate_mixing_block import (
    MultivariateBlock,
    MultivariateBlockConfig,
    TimeMixerConfig,
    VariateMixerConfig,
    variate_indices,
)
from iotdb.ainode.core.model.tirex2.cuda_graph import CUDAGraphRunner
from iotdb.ainode.core.model.tirex2.types import TimeseriesType

MatmulPrecision = Literal["highest", "high", "medium"]

logger = logging.getLogger(__file__)


def _resolve_act_func(spec: str) -> nn.Module:
    """Instantiate an activation module from its ``torch.nn`` name, such as ``SiLU`` or ``torch.nn.SiLU``.

    Only ``torch.nn`` modules are accepted, as the name comes from the checkpoint's config.json.
    """
    cls_name = spec.removeprefix("torch.nn.")
    act_cls = getattr(torch.nn, cls_name, None)
    if "." in cls_name or not (
        isinstance(act_cls, type) and issubclass(act_cls, nn.Module)
    ):
        raise ValueError(f"act_func must name a torch.nn module, got {spec!r}.")
    return act_cls()


@dataclass
class MultivariateStackConfig:
    """Named block templates with a per-position recipe for the multivariate block stack.

    Parameters
    ----------
    templates : dict[str, MultivariateBlockConfig]
        Named block configurations. Each key is a template name (e.g. ``"standard"``,
        ``"recurrent"``). ``block_idx`` and ``num_blocks`` must NOT be set here; they
        are injected by the backbone at stack construction time.
    recipe : list[str]
        One template name per block position (length must equal ``num_blocks``).
        Example: ``["standard", "recurrent", "standard", ...]``
    """

    templates: dict[str, MultivariateBlockConfig]
    recipe: list[str]

    @classmethod
    def from_dict(
        cls,
        config: dict,
        act_func: nn.Module,
        use_qk_norm: bool = True,
    ) -> "MultivariateStackConfig":
        """Build a stack config from a (possibly serialized) dict, injecting a shared activation.

        Each template may be given either as a fully-built
        :class:`MultivariateBlockConfig` (used as-is) or as a plain dict whose
        ``time_mixer``/``variate_mixer`` carry no ``act_fn``; in the latter case
        the provided ``act_func`` instance is injected into every mixer, matching
        how the backbone is configured with a single activation referenced
        throughout.

        Parameters
        ----------
        config : dict
            Stack config with keys ``templates`` (name -> :class:`MultivariateBlockConfig`
            or a template dict with ``time_mixer``, ``variate_mixer``, ``dropout``,
            ``eps``) and ``recipe`` (list of template names).
        act_func : nn.Module
            Activation instance shared across all mixers built from dict templates.
        use_qk_norm : bool
            Whether the attention variate mixers should RMS-normalize their
            query/key vectors. Injected into every mixer built from a dict
            template (default: True).

        The recurrent kernels follow the device of their inputs, so any
        serialized time mixer backend setting is ignored.
        """
        templates = {}
        for name, template in config["templates"].items():
            if isinstance(template, MultivariateBlockConfig):
                templates[name] = template
            else:
                time_mixer_config = dict(template["time_mixer"])
                time_mixer_config.pop("rnn_backend", None)
                templates[name] = MultivariateBlockConfig(
                    time_mixer=TimeMixerConfig(act_fn=act_func, **time_mixer_config),
                    variate_mixer=VariateMixerConfig(
                        act_fn=act_func,
                        use_qk_norm=use_qk_norm,
                        **template["variate_mixer"],
                    ),
                    dropout=template["dropout"],
                    eps=template["eps"],
                )
        return cls(templates=templates, recipe=list(config["recipe"]))


class TiRex2(nn.Module):
    def __init__(
        self,
        stack_config: dict,
        num_blocks: int,
        embedding_dim: int,
        input_patch_size: int,
        output_patch_size: int,
        quantiles: list[float],
        tokenizer_cfg: dict,
        scaler_cfg: dict,
        h_expand: float,
        context_len: int,
        future_len: int,
        input_ff_dim: int,
        act_func: str,
        dropout: float,
        *args,
        use_qk_norm: bool = True,
        stack_out_norm_config: dict | None = None,
        tta_sign_flip: bool = False,
        tta_diff: bool = True,
        matmul_precision: MatmulPrecision | None = None,
        **kwargs,
    ) -> None:
        if matmul_precision is not None:
            torch.set_float32_matmul_precision(matmul_precision)

        kwargs.pop("postprocessor_cfg", None)
        super().__init__(*args, **kwargs)
        act_func = _resolve_act_func(act_func)
        self.postprocessor = PostProcessor()
        self.stack_config = MultivariateStackConfig.from_dict(
            stack_config, act_func, use_qk_norm
        )

        if num_blocks == 0:
            raise ValueError("Cannot create a model without any blocks in the stack")

        assert (
            future_len % input_patch_size == 0 and input_patch_size == output_patch_size
        ), (
            "The future_len has to be integer divisible by the patch_size but "
            f"future_len%input_patch_size={future_len % input_patch_size} and input_patch_size has to "
            "be the same as output_patch_size (for inference) but "
            f"input_patch_size == output_patch_size={input_patch_size == output_patch_size}"
        )

        self.input_patch_size = input_patch_size
        self.output_patch_size = output_patch_size
        self.input_ff_dim = input_ff_dim
        self.act_func = act_func
        self.h_expand = h_expand
        self.context_len = context_len
        self.future_len = future_len
        self.embedding_dim = embedding_dim
        self.dropout = dropout

        # Input layer
        self.nan_mask_value = 0
        self.tokenizer = Tokenizer(**tokenizer_cfg)
        self.scaler = Scaler(**scaler_cfg)
        self.input_patch_embedding = ResidualBlock(
            in_dim=self.input_patch_size * 2,
            h_dim=self.input_ff_dim,
            out_dim=embedding_dim,
            act_fn=self.act_func,
            dropout_p=self.dropout,
        )

        if stack_out_norm_config is not None:
            eps = stack_out_norm_config.get("eps", 1e-7)
            use_rmsnorm = stack_out_norm_config.get("use_rmsnorm", False)
            if use_rmsnorm:
                self.stack_out_norm = torch.nn.RMSNorm(embedding_dim, eps=eps)
            else:
                self.stack_out_norm = RMSNorm(embedding_dim, eps=eps)
        else:
            self.stack_out_norm = torch.nn.Identity()

        # Multivariate stack
        self.stack: nn.ModuleList = self._create_stack(self.stack_config, num_blocks)

        # Output Layer
        self.num_quantiles = len(quantiles)
        quantiles_tensor = torch.tensor(quantiles)
        self.register_buffer("quantiles", quantiles_tensor, persistent=False)
        # TTA defaults for this checkpoint (carried in config.json).
        # ``predict`` uses them whenever the matching argument is left as None.
        self.tta_sign_flip = tta_sign_flip
        self.tta_diff = tta_diff
        # Lazily-built q -> 1-q index map for sign-flip TTA; see _complement_indices.
        self._complement_idx_cache: torch.Tensor | None = None
        self.output_patch_embedding = ResidualBlock(
            in_dim=embedding_dim,
            h_dim=int(embedding_dim * self.h_expand),
            out_dim=self.num_quantiles * self.output_patch_size,
            act_fn=self.act_func,
            dropout_p=self.dropout,
        )
        self.use_cuda_graphs = False
        self._cuda_graphs: CUDAGraphRunner | None = None

    def forward(
        self, batch: dict[str, Any]
    ) -> torch.Tensor | tuple[torch.Tensor, dict[str, torch.Tensor]]:
        """Forward pass producing quantile predictions for all variates in batch."""
        (
            x,
            group_vector,
            target_mask,
            known_covariate_mask,
            scaler_state,
            tokenizer_state,
        ) = self._prepare_forward(batch)

        if self._uses_cuda_graphs(x):
            x = self._run_cuda_graph(x, group_vector, target_mask, known_covariate_mask)
        else:
            x = self._build_stack_fn(group_vector, target_mask, known_covariate_mask)(x)

        x = torch.unflatten(
            x, -1, (self.num_quantiles, self.output_patch_size)
        )  # [B*V, L, D_out] -> [B*V, L, Q, P]
        x = torch.transpose(
            x, 1, 2
        )  # switch quantile and num_token_dimension  [B*V, L, Q, P] -> [B*V, Q, L, P]

        # reverse tokenization and scaling
        x = self.tokenizer.output_transform(
            x, tokenizer_state
        )  # [B*V, Q, L, P] -> [B*V, Q, T'], T' = T + padding
        x = self.scaler.re_scale(x, scaler_state)

        return x

    def _build_stack_fn(self, group_vector, target_mask, known_covariate_mask):
        """The stack as a function of the tokens; the mask indices are computed here, outside any CUDA graph."""
        indices = variate_indices(group_vector, target_mask, known_covariate_mask)
        return lambda x: self._stack(
            x, group_vector, target_mask, known_covariate_mask, indices
        )

    def _stack(self, x, group_vector, target_mask, known_covariate_mask, indices):
        """Block stack and output layer."""
        # forward through actual mLSTM/sLSTM layers, we select only the output of the last layer;
        # hidden representations are not relevant for forecasting
        for x in self._forward_layerwise(
            x, group_vector, target_mask, known_covariate_mask, indices
        ):
            pass

        # Normalise at the end of the stack
        x = self.stack_out_norm(x)
        x = nn.functional.dropout(x, self.dropout, training=self.training)
        return self.output_patch_embedding(x)  # [B*V, L, D] -> [B*V, L, D_out]

    def _run_cuda_graph(
        self, x, group_vector, target_mask, known_covariate_mask
    ) -> torch.Tensor:
        """Run the stack from a CUDA graph, captured per input shape.

        To share graphs, several variates are padded to a multiple of 4 with zero
        rows, each a target in a new group, so the real variates never see them.
        """
        if self._cuda_graphs is None:
            self._cuda_graphs = CUDAGraphRunner(self._build_stack_fn)

        num_rows = x.shape[0]
        num_pad = -num_rows % 4 if num_rows > 1 and group_vector is not None else 0
        if num_pad:
            x = nn.functional.pad(x, (0, 0, 0, 0, 0, num_pad))
            group_vector = torch.cat(
                [
                    group_vector,
                    group_vector.max() + 1 + torch.arange(num_pad, device=x.device),
                ]
            )
            target_mask = nn.functional.pad(target_mask, (0, num_pad), value=True)
            if known_covariate_mask is not None:
                known_covariate_mask = nn.functional.pad(
                    known_covariate_mask, (0, num_pad), value=False
                )
        return self._cuda_graphs(x, group_vector, target_mask, known_covariate_mask)[
            :num_rows
        ]

    def _uses_cuda_graphs(self, x: torch.Tensor) -> bool:
        """CUDA graphs serve gradient-free CUDA inference; FlexAttention's block mask can't be replayed.

        Graphs only pay off while kernel launches dominate, so larger batches run
        eagerly rather than holding a graph's buffers per shape.
        """
        return (
            self.use_cuda_graphs
            and x.is_cuda
            and x.shape[0] <= 32  # variate rows
            and not self.training
            and not torch.is_grad_enabled()
            and not any(
                block.variate_mixer.attn.use_flex_attention for block in self.stack
            )
        )

    def _apply(self, fn, *args, **kwargs):
        # The graphs read the parameters by address, which moving the model changes.
        self._cuda_graphs = None
        return super()._apply(fn, *args, **kwargs)

    def _forward_layerwise(
        self, x, group_vector, target_mask, known_covariate_mask, indices=None
    ):
        # Run the input through the stack.
        state = {i: None for i in range(len(self.stack))}
        for i, block in enumerate(self.stack):  # type: ignore[reportArgumentType]
            block_state = state[i]
            x, new_block_state = block(
                x,
                group_vector=group_vector,
                target_mask=target_mask,
                known_covariate_mask=known_covariate_mask,
                state=block_state,
                indices=indices,
            )

            if block_state is None:
                state[i] = new_block_state
            yield x

    def _prepare_forward(self, batch):
        x: torch.Tensor = batch["x"]
        group_vector: torch.Tensor | None = batch.get("group_vector", None)
        target_mask: torch.Tensor | None = batch.get("target_mask", None)

        # Determine which covariates are "known" (have non-NaN values in the future window).
        # Past-only covariates (all-NaN future) will be treated as forward-only by reverse_known_only.
        if target_mask is not None and x.shape[-1] >= self.future_len:
            has_future = ~torch.all(torch.isnan(x[:, -self.future_len :]), dim=-1)
            known_covariate_mask = has_future & ~target_mask
        else:
            known_covariate_mask = None

        # apply scaling and tokenize
        x, scaler_state = self.scaler.scale(x)
        x, tokenizer_state = self.tokenizer.input_transform(x)

        # embed the tokens with the "is-present" indication (i.e., x_mask)
        x_mask = (~torch.isnan(x)).to(x.dtype)
        x = torch.nan_to_num(x, nan=self.nan_mask_value)
        x = torch.cat((x, x_mask), dim=-1)
        x = self.input_patch_embedding(x)
        return (
            x,
            group_vector,
            target_mask,
            known_covariate_mask,
            scaler_state,
            tokenizer_state,
        )

    @torch.no_grad
    def predict(
        self,
        timeseries: list[TimeseriesType],
        prediction_length: int,
        *args,
        tta_sign_flip: bool | None = None,
        tta_diff: bool | None = None,
        **kwargs,
    ):
        """Return quantile forecasts aligned with the input sequence length.

        Parameters
        ----------
        timeseries : list[TimeseriesType]
            A batch of multivariate timeseries. Each :class:`TimeseriesType` holds
            the target and optional covariate tensors; the target has shape
            ``(num_variates, sequence_length)``.
        prediction_length : int
            The forecast horizon.
        tta_sign_flip : bool, optional
            Opt-in sign-flip test-time augmentation. Leave as ``None`` (the
            default) to use the checkpoint's configured setting
            (``self.tta_sign_flip``, from ``config.json``); pass an
            explicit ``True``/``False`` to override it for this call. When
            enabled, the model is run a second time on the sign-flipped input
            (target *and* every covariate negated); the flipped forecast is
            mapped back to level space (values negated and the quantile axis
            reversed via the ``q -> 1-q`` complement map) and the two passes
            are averaged in level space. Requires a symmetric quantile set
            and roughly doubles inference cost. When it resolves to disabled,
            the output is byte-identical to a single pass.
        tta_diff : bool, optional
            Opt-in differencing path inside the postprocessor. Leave as ``None``
            (the default) to use the checkpoint's configured setting
            (``self.tta_diff``, from ``config.json``); pass an explicit
            ``True``/``False`` to override trend differencing for this call.
        pad_context : bool, optional
            If ``True`` (the default), contexts shorter than ``context_len`` are
            left-padded with NaN up to the full model length. Pass ``False`` to
            skip padding to the full model length, reducing the number of input
            patches and inference time for short contexts at the cost of a small
            loss in forecast accuracy. Padding to the longest context in each
            batch and to a multiple of 4 patches still applies.
        """
        if tta_sign_flip is None:
            tta_sign_flip = self.tta_sign_flip
        if tta_diff is None:
            tta_diff = self.tta_diff

        forecasts = self._predict(
            timeseries, prediction_length, *args, tta_diff=tta_diff, **kwargs
        )
        # An empty batch has nothing to average (and no tensor to read a device
        # off of below), so short-circuit before the augmentation pass.
        if not tta_sign_flip or not forecasts:
            return forecasts

        # Treat predict end-to-end as a black box run twice: a second pass on
        # the sign-flipped series, mapped back to level space, then averaged.
        flipped = [self._sign_flip(ts) for ts in timeseries]
        forecasts_flip = self._predict(
            flipped, prediction_length, *args, tta_diff=tta_diff, **kwargs
        )

        complement = self._complement_indices().to(forecasts_flip[0].device)
        forecasts_flip = [-f.index_select(1, complement) for f in forecasts_flip]
        return [(a + b) / 2 for a, b in zip(forecasts, forecasts_flip)]

    def _prepare_input(
        self,
        timeseries,
        prediction_length,
        tta_diff,
        *args,
        pad_context: bool = True,
        **kwargs,
    ):
        """Run a single (un-augmented) forecast pass over the batch of series."""
        if self.postprocessor is None:
            raise RuntimeError("Cannot predict without postprocessor configured.")

        if prediction_length < 1:
            raise ValueError(
                f"prediction_length must be >= 1, got {prediction_length}."
            )

        if prediction_length > self.future_len:
            logger.warning(
                f"prediction_length={prediction_length} exceeds the supported maximum of "
                f"{self.future_len}. It will be truncated to {self.future_len}.",
            )
            prediction_length = self.future_len

        device = next(self.parameters()).device

        context = [ts.target.cpu() for ts in timeseries]
        past_covariates = [
            ts.past_covariates.cpu() if ts.past_covariates is not None else None
            for ts in timeseries
        ]
        past_future_covariates = []
        for ts in timeseries:
            if ts.future_covariates is None:
                past_future_covariates.append(None)
                continue
            expected_future_covariate_length = ts.target.shape[-1] + prediction_length
            future_covariates = ts.future_covariates
            if future_covariates.shape[-1] > expected_future_covariate_length:
                future_covariates = future_covariates[
                    ..., :expected_future_covariate_length
                ]
            past_future_covariates.append(future_covariates.cpu())

        batch, args, kwargs = self.postprocessor.transform_input(
            context,
            prediction_length,
            *args,
            past_covariates=past_covariates,
            past_future_covariates=past_future_covariates,
            tta_diff=tta_diff,
            **kwargs,
        )

        batch = {k: v.to(device) for k, v in batch.items()}
        # ensure that context size matches the model's expectations
        batch["x"] = self._fix_context_size(prediction_length, batch["x"], pad_context)
        return prediction_length, batch, args, kwargs

    def _fix_context_size(self, prediction_length, context, pad_context):
        right_pad = self.future_len - prediction_length
        context = nn.functional.pad(context, (0, right_pad), value=torch.nan)

        max_ts_len = self.context_len + self.future_len
        if context.shape[-1] < max_ts_len:
            # Without pad_context, pad to a multiple of 4 patches: fewer shapes to compile and capture.
            bucket = 4 * self.input_patch_size
            pad_to = (
                max_ts_len
                if pad_context
                else min(-(-context.shape[-1] // bucket) * bucket, max_ts_len)
            )
            context = nn.functional.pad(
                context, (pad_to - context.shape[-1], 0), value=torch.nan
            )
        elif context.shape[-1] > max_ts_len:
            context = context[..., -max_ts_len:]
        return context

    @staticmethod
    def _sign_flip(ts: TimeseriesType) -> TimeseriesType:
        """Negate the target and every covariate of a series for sign-flip TTA."""
        return TimeseriesType(
            target=-ts.target,
            past_covariates=None if ts.past_covariates is None else -ts.past_covariates,
            future_covariates=(
                None if ts.future_covariates is None else -ts.future_covariates
            ),
        )

    def _complement_indices(self, tol: float = 1e-6) -> torch.Tensor:
        """Index map sending each quantile ``q[i]`` to the one closest to ``1 - q[i]``.

        Sign-flip TTA reverses the quantile axis of the flipped forecast: the
        ``q`` quantile of ``-x`` is the ``1 - q`` quantile of ``x``. This builds
        ``complement`` with ``complement[i] = argmin_j |q[j] - (1 - q[i])|``;
        ``0.5`` maps to itself. Requires a symmetric quantile set - each
        complement must exist within ``tol`` or a :class:`ValueError` is raised.
        Cached on first use (it depends only on ``self.quantiles``).
        """
        if self._complement_idx_cache is not None:
            return self._complement_idx_cache

        q = self.quantiles.detach().to(dtype=torch.float32, device="cpu")
        complement = 1.0 - q
        indices = []
        for i in range(q.shape[0]):
            dist = torch.abs(q - complement[i])
            idx = int(torch.argmin(dist).item())
            if dist[idx].item() > tol:
                raise ValueError(
                    "tta_sign_flip requires a symmetric quantile set: quantile "
                    f"{q[i].item():.4f} has no complement (1 - q) within {tol} in "
                    f"{self.quantiles.tolist()}."
                )
            indices.append(idx)
        self._complement_idx_cache = torch.tensor(indices, dtype=torch.long)
        return self._complement_idx_cache

    def _predict(
        self,
        timeseries: list[TimeseriesType],
        prediction_length: int,
        *args,
        tta_diff: bool = True,
        preserve_grad: bool = False,
        pad_context: bool = True,
        **kwargs,
    ):
        """Run the model on all series inside context in parallel."""
        prediction_length, batch, args, kwargs = self._prepare_input(
            timeseries,
            prediction_length,
            tta_diff,
            *args,
            pad_context=pad_context,
            **kwargs,
        )

        pred = self(batch)
        # Drop the last token (predicts beyond sequence end); the remaining
        # tail of length future_len covers exactly the 10 future patches
        # that were directly supervised during training.
        pred = pred[..., : -self.output_patch_size]
        pred = pred[:, :, -self.future_len :]
        pred = pred[:, :, :prediction_length]

        result = self._prepare_output(
            timeseries,
            pred,
            prediction_length,
            *args,
            preserve_grad=preserve_grad,
            **kwargs,
        )

        return result

    def _prepare_output(
        self,
        original_timeseries,
        pred,
        prediction_length,
        *args,
        preserve_grad,
        **kwargs,
    ):

        output = self.postprocessor.transform_output(
            pred,
            prediction_length,
            *args,
            preserve_grad=preserve_grad,
            **kwargs,
        )  # type: ignore
        result = []
        for ts, out in zip(original_timeseries, output):
            result.append(out.sort(dim=1).values.to(ts.target))
        return result

    def _create_stack(
        self,
        config: MultivariateBlockConfig | MultivariateStackConfig,
        num_blocks: int,
    ) -> nn.ModuleList:
        """Create stack of multivariate blocks from configuration."""
        if isinstance(config, MultivariateStackConfig):
            templates = config.templates
            recipe = config.recipe
            if len(recipe) != num_blocks:
                raise ValueError(
                    f"block_recipe length ({len(recipe)}) must equal num_blocks ({num_blocks})"
                )
            unknown = set(recipe) - set(templates)
            if unknown:
                raise ValueError(
                    f"Recipe references unknown templates: {unknown}. Available: {set(templates)}"
                )
        else:
            # Backward-compatible path: repeat single config for all positions
            templates = {"default": config}
            recipe = ["default"] * num_blocks

        stack = []
        for block_idx, name in enumerate(recipe):
            template = templates[name]
            block_config = replace(template, block_idx=block_idx, num_blocks=num_blocks)
            stack.append(MultivariateBlock(block_config))
        return nn.ModuleList(stack)
