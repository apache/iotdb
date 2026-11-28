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

"""Single-pass, native-multivariate postprocessor with per-target differencing."""

import logging
from dataclasses import dataclass

import numpy as np
import torch
from torch.nn.functional import pad

logger = logging.getLogger(__name__)


@dataclass
class PostProcessorConfig:
    """Postprocessor defaults for trend detection and band calibration.

    These constants are the differencing/calibration values that used to be
    serialized in ``model/model-config.yaml``. Checkpoint configs now carry only
    ``tta_diff`` to enable or disable the differencing path at inference time.
    """

    r2: float = 0.87
    trend_min_r2: float = 0.87
    trend_threshold: float = 1.5
    sigma: float = 1.5
    trend_window: float = 1.0
    diff_band_exponent: float = 0.51
    diff_band_scale: float = 1.68
    raw_band_exponent: float = 0.52
    raw_band_scale: float = 0.90

    def __post_init__(self):
        if not 0.0 < self.trend_window <= 1.0:
            raise ValueError(
                f"trend_window must be a fraction in (0, 1], got {self.trend_window!r}."
            )
        if not self.diff_band_exponent >= 0.0:
            raise ValueError(
                f"diff_band_exponent must be >= 0, got {self.diff_band_exponent!r}."
            )
        if not self.diff_band_scale > 0.0:
            raise ValueError(
                f"diff_band_scale must be strictly positive, got {self.diff_band_scale!r}."
            )
        if not self.raw_band_exponent >= 0.0:
            raise ValueError(
                f"raw_band_exponent must be >= 0, got {self.raw_band_exponent!r}."
            )
        if not self.raw_band_scale > 0.0:
            raise ValueError(
                f"raw_band_scale must be strictly positive, got {self.raw_band_scale!r}."
            )
        if not (self.trend_min_r2 > 0.0 and self.trend_min_r2 <= 1.0):
            raise ValueError(
                f"trend_min_r2 ({self.trend_min_r2}) to be between 0 and 1."
            )
        if not self.trend_threshold > 0.0:
            raise ValueError(
                f"trend_threshold has to be larger than 0. Got {self.trend_threshold}."
            )


class PostProcessor:
    """Single-pass joint multivariate forecasting; trending targets are differenced."""

    def __init__(
        self, config: PostProcessorConfig | dict | None = None, *args, **kwargs
    ) -> None:
        if config is None:
            self.cfg = PostProcessorConfig()
        elif isinstance(config, dict):
            self.cfg = PostProcessorConfig(**config)
        else:
            self.cfg = config

    def transform_input(
        self,
        target: list[torch.Tensor],
        prediction_length: int,
        past_covariates: list[torch.Tensor],
        past_future_covariates: list[torch.Tensor],
        *args,
        tta_diff: bool = True,
        **kwargs,
    ):
        """Pack each sample into one joint group and optionally difference trending targets."""
        # ``past_covariates`` / ``past_future_covariates`` are fed to the model
        # but always on their raw scale - they are never differenced.
        target = [t.unsqueeze(0) if t.ndim == 1 else t for t in target]
        lengths = [t.shape[-1] for t in target for _ in range(t.shape[0])]
        num_rows = [t.shape[0] for t in target]
        max_len = max(lengths)

        # Pad if lengths of Targets is different
        if all(length == max_len for length in lengths):
            target_tensor = torch.cat(target, dim=0)
        else:
            target_tensor = torch.cat(
                [pad(t, (max_len - t.shape[-1], 0), value=torch.nan) for t in target],
                dim=0,
            )

        params = self._diff_params(target_tensor)
        should_diff = (
            self._detect_trends(target_tensor, lengths)
            if tta_diff
            else torch.zeros(target_tensor.shape[0], dtype=torch.bool)
        )

        row_left_pad = torch.tensor(
            [max_len - length for length in lengths], device=target_tensor.device
        )
        diff_t = (
            self._diff_forward(target_tensor, row_left_pad)
            if should_diff.any()
            else target_tensor
        )

        target_rows = torch.where(should_diff[:, None], diff_t, target_tensor)
        transform_params = [
            {"last_values": lv} for lv in params["last_values"].split(num_rows)
        ]
        diff_masks = [mask.tolist() for mask in should_diff.split(num_rows)]

        x = []
        group_vector = []
        target_mask = []
        group_sample_map = {}
        group_target_indices = {}
        group_row_is_diff = {}

        for sample_idx, (t_raw, sample_rows, diff_mask, pc, pfc) in enumerate(
            zip(
                target,
                target_rows.split(num_rows),
                diff_masks,
                past_covariates,
                past_future_covariates,
            )
        ):
            # One joint group per sample: its target rows (already left-padded to max_len,
            # differenced where a trend was found) plus its raw covariates.
            V_t = sample_rows.shape[0]

            pc_raw = (
                pc
                if pc is not None
                else torch.empty(
                    0, t_raw.shape[-1], device=t_raw.device, dtype=t_raw.dtype
                )
            )
            pfc_raw = (
                pfc
                if pfc is not None
                else torch.empty(
                    0,
                    t_raw.shape[-1] + prediction_length,
                    device=t_raw.device,
                    dtype=t_raw.dtype,
                )
            )
            if pc_raw.shape[-1] != t_raw.shape[-1]:
                raise ValueError(
                    "Past covariates and targets have to have the same length "
                    f"(expected {t_raw.shape[-1]}, got {pc_raw.shape[-1]})."
                )
            expected_future_covariate_length = t_raw.shape[-1] + prediction_length
            if pfc_raw.shape[-1] < expected_future_covariate_length:
                raise ValueError(
                    "Future known covariates must be at least as long as the target + the prediction length "
                    f"(expected at least {expected_future_covariate_length}, got {pfc_raw.shape[-1]})."
                )
            if pfc_raw.shape[-1] > expected_future_covariate_length:
                # Extra future-known covariate steps are harmless (e.g. when an
                # overlong requested horizon is capped); keep only what the model
                # consumes.
                pfc_raw = pfc_raw[..., :expected_future_covariate_length]
            V = V_t + pc_raw.shape[0] + pfc_raw.shape[0]

            next_group = (max(group_vector) if group_vector else 0) + 1
            group_sample_map[next_group] = sample_idx
            group_target_indices[next_group] = list(range(V_t))
            group_row_is_diff[next_group] = list(diff_mask)

            left_pad = max_len - t_raw.shape[-1]
            right_pad = prediction_length
            item = torch.concat(
                [
                    pad(sample_rows, (0, right_pad), value=torch.nan),
                    pad(pc_raw, (left_pad, right_pad), value=torch.nan),
                    pad(pfc_raw, (left_pad, 0), value=torch.nan),
                ],
                dim=0,
            )
            x.append(item)

            target_mask.extend([True] * V_t + [False] * (V - V_t))
            group_vector.extend([next_group] * V)

        device = target_tensor.device
        x = torch.concat(x, dim=0)
        group_vector = torch.tensor(group_vector, device=device, dtype=torch.float32)
        target_mask = torch.tensor(target_mask, device=device, dtype=torch.bool)

        return (
            {"x": x, "group_vector": group_vector, "target_mask": target_mask},
            [],
            {
                "group_sample_map": group_sample_map,
                "group_vector": group_vector,
                "target_mask": target_mask,
                "prediction_window_is_padded": True,
                "single_pass": True,
                "transform_params": transform_params,
                "diff_masks": diff_masks,
                "group_target_indices": group_target_indices,
                "group_row_is_diff": group_row_is_diff,
            },
        )

    def transform_output(
        self,
        output: torch.Tensor,
        *args,
        group_sample_map: dict[int, int],
        group_vector: torch.Tensor,
        target_mask: torch.Tensor,
        transform_params: list[dict],
        diff_masks: list[list[bool]],
        group_target_indices: dict[int, list[int]],
        group_row_is_diff: dict[int, list[bool]],
        preserve_grad: bool = False,
        **kwargs,
    ):
        """Calibrate the band per row type and integrate differenced rows back."""
        if not preserve_grad:
            output = output.detach().cpu()
        group_vector = group_vector.to(output.device)
        target_mask = target_mask.to(output.device)

        sample_rows = [[None] * len(mask) for mask in diff_masks]
        group_id = int(group_vector[0].item())

        while group_id <= group_vector.max().item():
            sample_idx = group_sample_map[group_id]
            target_indices = group_target_indices[group_id]
            mask = (group_vector == group_id) & target_mask
            group_sample = output[mask]

            # ``row_is_diff`` is per target row: differenced and raw rows can mix.
            row_is_diff = torch.tensor(
                group_row_is_diff[group_id], device=output.device, dtype=torch.bool
            )

            # Raw-row band calibration: sqrt(raw_band_scale) * t^(raw_band_exponent-0.5)
            # multiplies the model's per-step offset around its median.
            not_diff = ~row_is_diff
            if not_diff.any():
                q_med = group_sample.shape[1] // 2
                raw = group_sample[not_diff]
                median_step = raw[:, q_med : q_med + 1, :]
                offset = raw - median_step
                t_idx = torch.arange(
                    1, raw.shape[-1] + 1, device=raw.device, dtype=raw.dtype
                )
                horizon_factor = t_idx ** (self.cfg.raw_band_exponent - 0.5)
                rescaled = (
                    median_step
                    + (self.cfg.raw_band_scale**0.5) * horizon_factor * offset
                )
                group_sample = group_sample.clone()
                group_sample[not_diff] = rescaled

            if row_is_diff.any():
                last_values = transform_params[sample_idx]["last_values"][
                    target_indices
                ].to(output.device)
                if not preserve_grad:
                    last_values = last_values.detach()
                group_sample = self._diff_inverse(
                    group_sample,
                    row_is_diff,
                    {"last_values": last_values},
                    preserve_grad=preserve_grad,
                )

            for row, target_idx in zip(group_sample, target_indices):
                sample_rows[sample_idx][target_idx] = row

            group_id += 1

        return [torch.stack(rows, dim=0) for rows in sample_rows]

    def _detect_trends(self, x: torch.Tensor, lengths: list[int]) -> torch.Tensor:
        """Per-row trend flags from an ``ols_sigma`` fit on the last ``trend_window`` fraction of each row.

        ``x`` is ``[rows, T]``, left-padded with NaN; ``lengths`` holds each row's length before padding.
        Returns a bool tensor ``[rows]``.
        """
        width = x.shape[-1]
        windows = [max(1, round(self.cfg.trend_window * length)) for length in lengths]
        windows = torch.tensor(windows, dtype=torch.float64, device=x.device)

        pos = (
            torch.arange(width, dtype=torch.float64, device=x.device)[None, :]
            - (width - windows)[:, None]
        )
        values = x.to(torch.float64)
        valid = (pos >= 0) & ~torch.isnan(values)
        score, r2, std, n = self._ols_sigma_score(values, valid, pos, windows)
        return (
            (n >= 4)
            & (std >= 1e-12)
            & (score >= self.cfg.trend_threshold)
            & (r2 >= self.cfg.trend_min_r2)
        )

    @staticmethod
    def _ols_sigma_score(
        values: torch.Tensor,
        valid: torch.Tensor,
        pos: torch.Tensor,
        windows: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
        """``_ols_sigma_score`` for every row at once.

        values, pos: [rows, T] float64; valid: [rows, T] bool (in window and not NaN); windows: [rows].
        Returns score, r2, std and the number of valid values per row, each [rows].
        """
        n = valid.sum(-1, keepdim=True)  # valid values per row
        v = torch.where(valid, values, 0.0)  # NaN / outside window -> 0
        p = torch.where(valid, pos, 0.0)
        dv = torch.where(
            valid, v - v.sum(-1, keepdim=True) / n, 0.0
        )  # centred values, 0 where invalid
        dx = torch.where(
            valid, p - p.sum(-1, keepdim=True) / n, 0.0
        )  # centred positions
        s_xv = (dx * dv).sum(-1)
        s_xx = (dx * dx).sum(-1)
        s_vv = (dv * dv).sum(-1)
        n = n.squeeze(-1)
        std = torch.sqrt(s_vv / (n - 1))  # np.std(vals, ddof=1)
        score = (s_xv / s_xx / std).abs() * windows  # |slope_z| * len(row)
        r2 = s_xv * s_xv / (s_xx * s_vv)  # 1 - ss_res / ss_tot
        return score, r2, std, n

    @staticmethod
    def _diff_forward(x: torch.Tensor, left_pad: torch.Tensor) -> torch.Tensor:
        """First-order row-wise differencing of left-padded rows ``[rows, T]``.

        ``left_pad`` holds each row's number of leading padding columns: the padding stays NaN and
        each row's first real step is 0.
        """
        diff = torch.full_like(x, torch.nan)
        diff[:, 1:] = x[:, 1:] - x[:, :-1]
        diff[torch.arange(x.shape[0], device=x.device), left_pad] = 0.0
        return diff

    @staticmethod
    def _diff_params(x: torch.Tensor) -> dict:
        """Retain last observed levels (NaN-safe, falling back to 0.0) for inversion."""
        not_nan = ~torch.isnan(x)
        last_idx = not_nan.long().cumsum(dim=-1).argmax(dim=-1)
        last_values = x.gather(-1, last_idx.unsqueeze(-1)).squeeze(-1)
        return {"last_values": torch.nan_to_num(last_values, nan=0.0)}

    def _diff_inverse(
        self,
        sample: torch.Tensor,
        mask: torch.Tensor,
        params: dict,
        preserve_grad: bool = False,
    ) -> torch.Tensor:
        """Invert first-order differencing on ``mask`` rows: median path + rss band term."""
        out = sample.clone()
        if not mask.any():
            return out
        constants = params["last_values"]
        broadcast_shape = (-1,) + (1,) * (sample.ndim - 1)
        diff = sample[mask]

        q_med = diff.shape[1] // 2
        median_step = diff[:, q_med : q_med + 1, :]
        median_path = torch.cumsum(median_step, dim=-1)
        offset = diff - median_step

        sign = offset.sum(dim=-1, keepdim=True).sign()
        rss_squared = torch.cumsum(offset * offset, dim=-1)
        if preserve_grad:
            # The median offset is identically zero. Avoid sqrt'(0) in backward
            # while keeping its forward value exactly zero.
            rss = torch.sqrt(rss_squared.clamp_min(torch.finfo(rss_squared.dtype).tiny))
            rss = torch.where(rss_squared > 0, rss, torch.zeros_like(rss))
        else:
            rss = torch.sqrt(rss_squared)
        t_idx = torch.arange(
            1, diff.shape[-1] + 1, device=diff.device, dtype=diff.dtype
        )
        horizon_factor = t_idx ** (self.cfg.diff_band_exponent - 0.5)
        band = sign * rss * horizon_factor

        recon = median_path + (self.cfg.diff_band_scale**0.5) * band
        out[mask] = recon + constants[mask].reshape(broadcast_shape)
        return out
