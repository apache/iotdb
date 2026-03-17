import json
import os
from typing import NamedTuple, Optional

import torch
import torch.nn as nn
from torch.nn.parameter import Parameter

from standalone_finetune.model.model_constants import ADAPTER_CONFIG, ADAPTER_PT
from standalone_finetune.finetune.adapter.weaver.base_weaver import (
    WeaverConfig,
    get_weaver,
)


class ModelOutput(NamedTuple):
    """Output structure for DualWeaver to be compatible with Trainer."""

    loss: torch.Tensor
    predictions: Optional[torch.Tensor] = None


class DualWeaver(nn.Module):
    """
    DualWeaver model:
        1. Dual-path transformation: x1 = a*x + weaver(x), x2 = -b*x + weaver(x)
        2. Inference inverse transform: pred = (y1 - y2) / (a + b)
        3. Special loss: loss1 + loss2 + max(2*(loss1+loss2)/(a+b)^2, loss_origin)

    Mathematical principle:
        Assuming LTM (Large Time-series Model) is approximately linear:
        y1 = LTM(a*x + w(x)) ≈ a*LTM(x) + LTM(w(x))
        y2 = LTM(-b*x + w(x)) ≈ -b*LTM(x) + LTM(w(x))
        y1 - y2 ≈ (a+b)*LTM(x)
        LTM(x) ≈ (y1 - y2) / (a + b)

    Features:
        1. Learnable weights a, b are [1, input_channel] vectors
        2. Supports Instance Normalization (RevIN)
        3. Special loss function for training stability
    """

    def __init__(
        self,
        config: WeaverConfig,
        ltm: nn.Module,
        model_type: str = "sundial",
        test_pred_len: int = 96,
        test_n_sample: int = 20,
    ):
        """
        Initialize DualWeaver.

        Args:
            config: Weaver configuration
            ltm: Large Time-series Model
            model_type: Base model type ("sundial", "timer", "chronos")
            test_pred_len: Prediction length for testing
            test_n_sample: Number of samples for probabilistic prediction
        """
        super().__init__()
        self.config = config
        self.ltm = ltm
        self.model_type = model_type.lower()
        self.test_pred_len = test_pred_len
        self.test_n_sample = test_n_sample

        self.feature_weaver = get_weaver(config)

        self.a = Parameter(torch.ones(1, config.input_channel))
        self.b = Parameter(torch.ones(1, config.input_channel))

        self.criterion = nn.MSELoss()

    # ---- Properties delegated to the base model (HuggingFace compat) ----

    @property
    def device(self) -> torch.device:
        """Device of the underlying base model."""
        return self.ltm.device

    @property
    def dtype(self) -> torch.dtype:
        """Dtype of the underlying base model."""
        return next(self.ltm.parameters()).dtype

    def forward(
        self,
        input_ids: torch.Tensor,
        labels: Optional[torch.Tensor] = None,
        loss_masks: Optional[torch.Tensor] = None,
        mask_y: Optional[torch.Tensor] = None,
        **kwargs,
    ) -> ModelOutput:
        """
        Forward pass (unified HuggingFace-style interface).

        Args:
            input_ids: Input sequence [B, seq_len, C]  (aliased from batch_x)
            labels: Target sequence [B, pred_len, C]    (aliased from batch_y, required for training)
            loss_masks: Loss mask [B, token_num, C]     (optional)
            mask_y: Output mask [B, output_token_len, C] (optional)

        Returns:
            ModelOutput with loss and optional predictions

        Note:
            Training vs inference is determined by ``self.training`` (set via
            ``model.train()`` / ``model.eval()``), consistent with HF models.
        """
        batch_x = input_ids
        batch_y = labels

        # Ensure 3D: [B, L] -> [B, L, 1] for univariate data
        if batch_x.dim() == 2:
            batch_x = batch_x.unsqueeze(-1)
        if batch_y is not None and batch_y.dim() == 2:
            batch_y = batch_y.unsqueeze(-1)
        if mask_y is not None and mask_y.dim() == 2:
            mask_y = mask_y.unsqueeze(-1)

        # Instance Normalization (RevIN)
        means = batch_x.mean(1, keepdim=True).detach()
        stdev = batch_x.std(dim=1, keepdim=True, unbiased=False).detach()
        stdev = torch.where(
            stdev > 1e-2, stdev, torch.tensor(1e-2, device=batch_x.device)
        )

        batch_x_norm = (batch_x - means) / stdev

        # Original input (use last channel only - target variable)
        x0 = batch_x_norm[:, :, -1:]  # [B, seq_len, 1]
        x0 = x0.permute(0, 2, 1)  # [B, 1, seq_len]
        x0 = x0.reshape(-1, x0.shape[-1])  # [B*1, seq_len]

        # Dual-path transformation
        # NOTE: feature_weaver is called TWICE (once per path) on purpose.
        # During training, Dropout produces different masks each call, acting
        # as extra regularisation — consistent with the reference implementation.
        x1 = self.a * batch_x_norm + self.feature_weaver(
            batch_x_norm, is_output_seq=False
        )  # [B, seq_len, C]
        x2 = -self.b * batch_x_norm + self.feature_weaver(
            batch_x_norm, is_output_seq=False
        )  # [B, seq_len, C]
        x1 = x1[:, :, -1:]  # [B, seq_len, 1] - use last channel (target)
        x2 = x2[:, :, -1:]

        # Batch doubling
        batch_x_cat = torch.cat([x1, x2], dim=0)  # [2*B, seq_len, 1]
        B = batch_x_cat.shape[0]  # 2 * original_B
        M = batch_x_cat.shape[-1]  # 1
        batch_x_cat = batch_x_cat.permute(0, 2, 1)  # [2*B, 1, seq_len]
        batch_x_cat = batch_x_cat.reshape(-1, batch_x_cat.shape[-1])  # [2*B*1, seq_len]

        # Determine mode by whether labels are provided (HuggingFace convention):
        #   labels provided  → compute loss  (training + validation)
        #   labels absent    → inference only (generate predictions)
        if batch_y is not None:
            loss = self._train_forward(
                batch_x_cat, batch_y, batch_x_norm, x0, means, stdev, mask_y, B, M
            )
            return ModelOutput(loss=loss)
        else:
            predictions = self._inference_forward(batch_x_cat, means, stdev, B, M)
            return ModelOutput(
                loss=torch.tensor(0.0, device=batch_x.device), predictions=predictions
            )

    def _train_forward(
        self,
        batch_x: torch.Tensor,
        batch_y: torch.Tensor,
        batch_x_norm: torch.Tensor,
        x0: torch.Tensor,
        means: torch.Tensor,
        stdev: torch.Tensor,
        mask_y: Optional[torch.Tensor],
        B: int,
        M: int,
    ) -> torch.Tensor:
        """Training forward pass."""

        # Process batch_y
        batch_y_norm = (batch_y - means) / stdev

        y0 = batch_y_norm[:, :, -1:]  # [B/2, pred_len, 1]
        y0 = y0.permute(0, 2, 1)  # [B/2, 1, pred_len]
        y0 = y0.reshape(-1, y0.shape[-1])  # [B/2*1, pred_len]

        y1 = self.a * batch_y_norm + self.feature_weaver(
            batch_y_norm, is_output_seq=True
        )
        y2 = -self.b * batch_y_norm + self.feature_weaver(
            batch_y_norm, is_output_seq=True
        )
        y1 = y1[:, :, -1:]
        y2 = y2[:, :, -1:]

        batch_y_cat = torch.cat([y1, y2], dim=0)  # [B, pred_len, 1]
        batch_y_cat = batch_y_cat.permute(0, 2, 1)  # [B, 1, pred_len]
        batch_y_cat = batch_y_cat.reshape(-1, batch_y_cat.shape[-1])  # [B*1, pred_len]

        # Process mask_y (position-based mask from dataset, if provided)
        # DualWeaver only uses the last channel, so extract mask for that channel.
        if mask_y is not None:
            mask_y = mask_y[:, :, -1:]  # [B_orig, pred_len, 1]
            mask_y = torch.cat([mask_y, mask_y], dim=0)  # [2*B_orig, pred_len, 1]
            mask_y = mask_y.squeeze(-1)  # [2*B_orig, pred_len]

        # Model-specific forward
        if "timer" in self.model_type:
            return self._timer_train(batch_x, batch_y_cat, x0, y0, B, M)
        elif "sundial" in self.model_type:
            return self._sundial_train(batch_x, batch_y_cat, x0, y0, mask_y, B, M)
        elif "chronos" in self.model_type:
            return self._chronos_train(batch_x, batch_y_cat, x0, y0, B, M)
        else:
            raise NotImplementedError(f"Model type {self.model_type} not supported")

    def _timer_train(
        self,
        batch_x: torch.Tensor,
        batch_y: torch.Tensor,
        x0: torch.Tensor,
        y0: torch.Tensor,
        B: int,
        M: int,
    ) -> torch.Tensor:
        """Timer model training forward.

        Uses ``reduction='none'`` so Timer returns per-sample losses
        (shape [bsz, n_tokens]) instead of a scalar, enabling the
        dual-path loss splitting via ``torch.chunk``.
        """
        outputs = self.ltm(input_ids=batch_x, labels=batch_y, reduction="none")
        losses = outputs["loss"] if isinstance(outputs, dict) else outputs[0]

        with torch.no_grad():
            outputs_origin = self.ltm(input_ids=x0, labels=y0, reduction="none")
            loss_origin = (
                outputs_origin["loss"]
                if isinstance(outputs_origin, dict)
                else outputs_origin[0]
            )

        loss1, loss2 = torch.chunk(losses, 2, dim=0)
        loss1 = loss1.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)
        loss2 = loss2.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)

        loss_origin = loss_origin.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)
        loss_origin = loss_origin[:, -1:]

        loss1 = loss1[:, -1:]
        loss2 = loss2[:, -1:]

        loss = (
            loss1.mean()
            + loss2.mean()
            + torch.max(
                (2 * (loss1 + loss2) / (self.a[:, -1:] + self.b[:, -1:]) ** 2).mean(),
                loss_origin.mean().detach(),
            )
        )
        return loss

    def _sundial_train(
        self,
        batch_x: torch.Tensor,
        batch_y: torch.Tensor,
        x0: torch.Tensor,
        y0: torch.Tensor,
        mask_y: Optional[torch.Tensor],
        B: int,
        M: int,
    ) -> torch.Tensor:
        """
        Sundial model training forward.

        Uses ``reduction='none'`` so Sundial returns per-sample losses
        (shape [bsz * L]) instead of a scalar.  The standard Sundial
        ``FlowLoss`` averages over ``diffusion_batch_mul`` copies internally
        when ``reduce=False``.

        ``loss_masks`` is required by standard Sundial (per-token mask,
        shape [batch, token_num]).  DualWeaver restructures batches so the
        original dataset masks don't apply; we use all-ones.
        """
        token_num = self.config.seq_len // self.config.input_token_len
        loss_masks_main = torch.ones(batch_x.shape[0], token_num, device=batch_x.device)
        loss_masks_origin = torch.ones(x0.shape[0], token_num, device=x0.device)

        outputs = self.ltm(
            input_ids=batch_x,
            labels=batch_y,
            loss_masks=loss_masks_main,
            mask_y=mask_y,
            reduction="none",
        )
        losses = outputs["loss"] if isinstance(outputs, dict) else outputs[0]

        with torch.no_grad():
            outputs_origin = self.ltm(
                input_ids=x0,
                labels=y0,
                loss_masks=loss_masks_origin,
                mask_y=torch.chunk(mask_y, 2, dim=0)[0] if mask_y is not None else None,
                reduction="none",
            )
            loss_origin = (
                outputs_origin["loss"]
                if isinstance(outputs_origin, dict)
                else outputs_origin[0]
            )

        loss1, loss2 = torch.chunk(losses, 2, dim=0)
        loss1 = loss1.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)
        loss2 = loss2.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)

        loss1 = loss1[:, -1:]
        loss2 = loss2[:, -1:]

        loss_origin = loss_origin.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)
        loss_origin = loss_origin[:, -1:]

        loss = (
            loss1.mean()
            + loss2.mean()
            + torch.max(
                (2 * (loss1 + loss2) / (self.a[:, -1:] + self.b[:, -1:]) ** 2).mean(),
                loss_origin.mean().detach(),
            )
        )
        return loss

    def _chronos_train(
        self,
        batch_x: torch.Tensor,
        batch_y: torch.Tensor,
        x0: torch.Tensor,
        y0: torch.Tensor,
        B: int,
        M: int,
    ) -> torch.Tensor:
        """Chronos model training forward.

        Uses ``reduction='none'`` so Chronos returns per-sample losses
        (shape [batch]) instead of a scalar.
        """
        assert self.config.input_token_len == self.config.seq_len
        assert self.config.max_output_token_len == self.test_pred_len

        group_ids = torch.arange(B).repeat_interleave(M).to(self.ltm.device)
        outputs = self.ltm(
            context=batch_x,
            group_ids=group_ids,
            num_output_patches=self.test_pred_len // 16,
            future_target=batch_y[:, -self.test_pred_len :],
            reduction="none",
        )
        losses = outputs.loss

        with torch.no_grad():
            group_ids_origin = (
                torch.arange(B // 2).repeat_interleave(M).to(self.ltm.device)
            )
            outputs_origin = self.ltm(
                context=x0,
                group_ids=group_ids_origin,
                num_output_patches=self.test_pred_len // 16,
                future_target=y0[:, -self.test_pred_len :],
                reduction="none",
            )
            loss_origin = (
                outputs_origin.loss.reshape(B // 2, M, -1)
                .permute(0, 2, 1)
                .reshape(-1, M)[:, -1:]
                .mean()
            )

        loss1, loss2 = torch.chunk(losses, 2, dim=0)
        loss1 = loss1.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)
        loss2 = loss2.reshape(B // 2, M, -1).permute(0, 2, 1).reshape(-1, M)

        loss1 = loss1[:, -1:]
        loss2 = loss2[:, -1:]

        loss = (
            loss1.mean()
            + loss2.mean()
            + torch.max(
                (2 * (loss1 + loss2) / (self.a[:, -1:] + self.b[:, -1:]) ** 2).mean(),
                loss_origin.detach(),
            )
        )
        return loss

    def _inference_forward(
        self,
        batch_x: torch.Tensor,
        means: torch.Tensor,
        stdev: torch.Tensor,
        B: int,
        M: int,
    ) -> torch.Tensor:
        """Inference forward pass."""

        if "timer" in self.model_type:
            outputs = self.ltm.generate(batch_x, max_new_tokens=self.test_pred_len)
            predictions = outputs.reshape(B, M, -1)  # [B, 1, pred_len]
            predictions = predictions.permute(0, 2, 1)  # [B, pred_len, 1]

            y1, y2 = torch.chunk(predictions, 2, dim=0)  # each [B_orig, pred_len, 1]
            predictions = (y1 - y2) / (self.a[:, -1:] + self.b[:, -1:])
            predictions = predictions * stdev[:, :, -1:] + means[:, :, -1:]

        elif "sundial" in self.model_type:
            # ltm.generate returns [2*B_orig, N, pred_len] (no revin – DualWeaver handles it)
            outputs = self.ltm.generate(
                batch_x,
                max_new_tokens=self.test_pred_len,
                num_samples=self.test_n_sample,
                revin=False,
            )
            # reshape: [2*B_orig, M, N, pred_len] where M=1
            predictions = outputs.reshape(B, M, outputs.shape[1], outputs.shape[-1])
            predictions = predictions.permute(0, 2, 3, 1)  # [B, N, pred_len, M]

            y1, y2 = torch.chunk(predictions, 2, dim=0)  # each [B_orig, N, pred_len, M]
            predictions = (y1 - y2) / (self.a[:, -1:] + self.b[:, -1:])

            # denormalize: permute so N is first for broadcasting with stdev [B_orig, 1, C]
            predictions = predictions.permute(1, 0, 2, 3)  # [N, B_orig, pred_len, M]
            predictions = predictions * stdev[:, :, -1:] + means[:, :, -1:]
            predictions = predictions.permute(1, 0, 2, 3)  # [B_orig, N, pred_len, M]
            predictions = predictions.squeeze(-1)  # [B_orig, N, pred_len]

        elif "chronos" in self.model_type:
            assert self.config.input_token_len == self.config.seq_len
            assert self.config.max_output_token_len == self.test_pred_len

            group_ids = torch.arange(B).repeat_interleave(M).to(self.ltm.device)
            outputs = self.ltm(
                context=batch_x,
                group_ids=group_ids,
                num_output_patches=self.test_pred_len // 16,
                future_target=None,
            )
            predictions = outputs.quantile_preds[
                :, :, : self.test_pred_len
            ]  # [B*C, N, L]

            predictions_1, predictions_2 = torch.chunk(predictions, 2, dim=0)
            # mean over num_samples → [B_orig, pred_len], reshape → [B_orig, 1, pred_len]
            predictions_1 = (
                predictions_1.mean(dim=1).reshape(B // 2, M, -1).permute(0, 2, 1)
            )
            predictions_2 = (
                predictions_2.mean(dim=1).reshape(B // 2, M, -1).permute(0, 2, 1)
            )
            # [B_orig, pred_len, 1] / [1, 1] → [B_orig, pred_len, 1]
            predictions = (predictions_1 - predictions_2) / (
                self.a[:, -1:] + self.b[:, -1:]
            )
            predictions = predictions * stdev[:, :, -1:] + means[:, :, -1:]
        else:
            raise NotImplementedError(f"Model type {self.model_type} not supported")

        return predictions

    @torch.inference_mode()
    def generate(
        self,
        batch_x: torch.Tensor,
        max_new_tokens: Optional[int] = None,
        num_samples: Optional[int] = None,
        **kwargs,
    ) -> torch.Tensor:
        """
        Generate predictions (inference mode).

        Args:
            batch_x: Input sequence [B, seq_len, C]
            max_new_tokens: Override prediction length
            num_samples: Override number of samples for probabilistic prediction
            **kwargs: Extra args from the pipeline (e.g. ``revin``) are
                      accepted but ignored – DualWeaver handles RevIN internally.

        Returns:
            Predictions tensor
        """
        if max_new_tokens is not None:
            self.test_pred_len = max_new_tokens
        if num_samples is not None:
            self.test_n_sample = num_samples

        # Ensure 3D for generate() callers
        if batch_x.dim() == 2:
            batch_x = batch_x.unsqueeze(-1)

        was_training = self.training
        self.eval()
        output = self.forward(input_ids=batch_x)
        if was_training:
            self.train()
        return output.predictions

    # ==================== Persistence ====================

    def save_pretrained(self, output_dir: str) -> None:
        """Save DualWeaver adapter weight.

        Like LoRA, only the adapter increment is persisted.  The base
        model weights are loaded from the original base model directory
        at inference time.

        Directory layout::
            output_dir/
            ├── adapter_config.json     # WeaverConfig + DualWeaver hyper-parameters
            └── adapter_model.pt        # only a, b, feature_weaver (small)
        """
        os.makedirs(output_dir, exist_ok=True)

        # 1. Save adapter weights only (a, b, feature_weaver)
        adapter_state = {
            "a": self.a.data,
            "b": self.b.data,
            "feature_weaver": self.feature_weaver.state_dict(),
        }
        torch.save(adapter_state, os.path.join(output_dir, ADAPTER_PT))

        # 2. Save adapter config (needed to reconstruct DualWeaver at load time)
        adapter_config = {
            "adapter_type": "dual_weaver",
            "weaver_config": self.config.to_dict(),
            "model_type": self.model_type,
            "test_pred_len": self.test_pred_len,
            "test_n_sample": self.test_n_sample,
        }
        with open(os.path.join(output_dir, ADAPTER_CONFIG), "w", encoding="utf-8") as f:
            json.dump(adapter_config, f, indent=2)

    @classmethod
    def from_pretrained(
        cls,
        adapter_dir: str,
        ltm: nn.Module,
        device: Optional[torch.device] = None,
    ) -> "DualWeaver":
        """Reconstruct a DualWeaver from a saved adapter directory.

        Args:
            adapter_dir: containing adapter_config.json and adapter_model.pt .
            ltm: The base model which is loaded from the base model directory.
            device: Device to map adapter weights onto.

        Returns:
            Fully initialised DualWeaver with adapter weights restored.
        """
        config_path = os.path.join(adapter_dir, ADAPTER_CONFIG)
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"{ADAPTER_CONFIG} not found in {adapter_dir}")

        weights_path = os.path.join(adapter_dir, ADAPTER_PT)
        if not os.path.exists(weights_path):
            raise FileNotFoundError(f"{ADAPTER_PT} not found in {adapter_dir}")

        with open(config_path, "r", encoding="utf-8") as f:
            adapter_cfg = json.load(f)

        weaver_config = WeaverConfig.from_dict(adapter_cfg["weaver_config"])

        model = cls(
            config=weaver_config,
            ltm=ltm,
            model_type=adapter_cfg.get("model_type", "sundial"),
            test_pred_len=adapter_cfg.get("test_pred_len", 96),
            test_n_sample=adapter_cfg.get("test_n_sample", 20),
        )

        # Restore adapter weights (a, b, feature_weaver)
        adapter_state = torch.load(weights_path, map_location=device or "cpu")
        model.a.data.copy_(adapter_state["a"])
        model.b.data.copy_(adapter_state["b"])
        model.feature_weaver.load_state_dict(adapter_state["feature_weaver"])

        return model
