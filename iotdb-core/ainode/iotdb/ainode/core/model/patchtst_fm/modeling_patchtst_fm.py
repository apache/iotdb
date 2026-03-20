# Copyright contributors to the TSFM project
#
"""PatchTST-FM model implementation"""

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F
from einops import rearrange
from transformers.modeling_utils import PreTrainedModel
from transformers.utils import ModelOutput, logging

from .basic import (
    TransformerBlock,
    make_attn_mask,
)
from .configuration_patchtst_fm import PatchTSTFMConfig
from .normalization import RevIN
from .tools import count_parameters


logger = logging.get_logger(__name__)


class LearnedPositionalEmbedding(nn.Module):
    def __init__(self, d_model, max_len=5000, type="add"):
        super().__init__()
        self.embedding = nn.Embedding(max_len, d_model)
        self.type = type

    def forward(self, x):
        positions = torch.arange(x.size(-2), device=x.device).unsqueeze(0)
        pe = self.embedding(positions)
        if x.ndim == 4:
            pe = pe.unsqueeze(1)
        if self.type == "add":
            return x + pe
        elif self.type == "mul":
            return x * pe
        else:
            raise ValueError(f"Invalid type: {self.type}")


class ResidualBlock(nn.Module):
    def __init__(self, d_in, d_out, d_hidden):
        super().__init__()

        self.layer1 = nn.Linear(d_in, d_hidden)
        self.layer2 = nn.Linear(d_hidden, d_out)
        self.residual = nn.Linear(d_in, d_out)
        self.activation = nn.Sigmoid()

    def forward(self, x):
        return self.layer2(self.activation(self.layer1(x))) + self.residual(x)


class PatchTSTFMPreTrainedModel(PreTrainedModel):
    # Weight initialization
    config_class = PatchTSTFMConfig
    base_model_prefix = "model"
    main_input_name = "inputs"
    supports_gradient_checkpointing = False


@dataclass
class PatchTSTFMModelOutput(ModelOutput):
    loss_mask: torch.Tensor = None
    normed_target: torch.Tensor = None
    hidden_states: Optional[Tuple[torch.FloatTensor]] = None
    quantile_predictions: torch.FloatTensor = None


@dataclass
class PatchTSTFMPretrainingOutput(ModelOutput):
    loss: torch.Tensor = None
    hidden_states: Optional[Tuple[torch.FloatTensor]] = None
    quanitle_predictions: torch.Tensor = None


@dataclass
class PatchTSTFMPredictionOutput(ModelOutput):
    hidden_states: Optional[Tuple[torch.FloatTensor]] = None
    quantile_predictions: torch.Tensor = None


class PatchTSTFMModel(PatchTSTFMPreTrainedModel):
    def __init__(self, config: PatchTSTFMConfig):
        super().__init__(config)
        self.config = config
        self.quantile_levels = config.quantile_levels
        self.pos_embed = LearnedPositionalEmbedding(d_model=config.d_model, max_len=config.n_patch, type="add")
        assert config.d_model % config.n_head == 0, "[QuantileDecoder] d_model must be divisible by n_head"

        self.blocks = nn.ModuleList(
            [
                TransformerBlock(
                    config.d_model,
                    config.n_head,
                    mlp_ratio=4.0,
                    norm_first=True,
                    dropout=0.1,
                )
                for _ in range(config.n_layer)
            ]
        )
        self.in_layer = ResidualBlock(config.d_patch * 2, config.d_model, config.d_model)
        self.out_layer = ResidualBlock(config.d_model, config.d_patch * (config.num_quantile + 1), config.d_model)

        self.norm_fn = RevIN(dim=-1, std_min=1e-5, use_sinh=True)

    def model_summary(self):
        s = ""
        model_name = "PatchTST-FM"
        s += f"{'=' * 5:<10} {model_name} {'=' * 5:>9}\n"
        s += f"{'Transformer:':<20} {count_parameters(self.blocks)[0] / 1e6:>8.2f}M\n"
        s += f"{'=' * 30}\n"
        p = count_parameters(self)
        s += f"{'Trainable:':<20} {p[1] / 1e6:>8.2f}M\n"
        s += f"{'Frozen:':<20} {p[2] / 1e6:>8.2f}M\n"
        s += f"{'Total:':<20} {p[0] / 1e6:>8.2f}M\n"
        s += f"{'=' * 30}\n"
        return s

    def forward(
        self,
        inputs: torch.Tensor,
        pred_mask: torch.Tensor,
        miss_mask: torch.Tensor,
        pad_mask: torch.Tensor,
        output_hidden_states: Optional[bool] = False,
        return_loss: bool = True,
        return_dict: Optional[bool] = None,
        # **kwargs,
    ) -> PatchTSTFMPretrainingOutput:
        x = inputs.to(self.device)
        pad_mask = pad_mask.to(self.device).bool()
        pred_mask = pred_mask.to(self.device).bool()
        miss_mask = miss_mask.to(self.device).bool()
        if x.ndim > 2:
            x = rearrange(x, "B N T -> (B N) T")
            pad_mask = rearrange(pad_mask, "B N T -> (B N) T")
            pred_mask = rearrange(pred_mask, "B N T -> (B N) T")
            miss_mask = rearrange(miss_mask, "B N T -> (B N) T")

        B, T = x.shape
        ts_mask = pred_mask | pad_mask | miss_mask

        x_target = self.norm_fn.fit_transform(x, mask=pred_mask | pad_mask | miss_mask)
        x_input = torch.where(ts_mask, torch.zeros_like(x_target), x_target)

        x_patch = x_input.reshape(B, self.config.n_patch, self.config.d_patch)
        mask_patch = ts_mask.reshape(B, self.config.n_patch, self.config.d_patch)
        pad_patch_mask = pad_mask.reshape(B, self.config.n_patch, self.config.d_patch).float().mean(dim=-1).gt(0.9)

        q_pred, q_raw = self.decode(x=x_patch, mask=mask_patch.float(), t_pad_mask=pad_patch_mask)
        q_pred = q_pred.permute(0, 2, 3, 1)

        B, N, D, Q = q_pred.shape
        q_pred = q_pred.reshape(B, N * D, Q)

        if output_hidden_states:
            hidden_states = q_raw.reshape(B, N * D, Q)
        else:
            hidden_states = None

        # return here q_pred, loss_mask, and x_target
        return PatchTSTFMModelOutput(
            normed_target=x_target,
            quantile_predictions=q_pred,
            loss_mask=(pred_mask & ~pad_mask & ~miss_mask).float(),
            hidden_states=hidden_states,
        )

    def decode(self, x, mask, t_pad_mask=None):
        B, N, D = x.shape
        # x = self.in_layer(torch.cat([x, t, 1 - mask], dim=-1))
        x = self.in_layer(torch.cat([x, 1 - mask], dim=-1))
        pad_attn_mask = make_attn_mask(t_pad_mask, t_pad_mask).unsqueeze(1)

        x = self.pos_embed(x)
        for block in self.blocks:
            x = block(x, pad_attn_mask)
        x = self.out_layer(x)
        q_raw = x.reshape(B, N, self.config.num_quantile + 1, self.config.d_patch).permute(0, 2, 1, 3)
        q = q_raw[:, 0, :, :].unsqueeze(1) + torch.cumsum(
            F.softplus(q_raw[:, 1:, :, :]) / self.config.num_quantile, dim=1
        )
        return q, q_raw


class PatchTSTFMForPretraining(PatchTSTFMPreTrainedModel):
    def __init__(self, config: PatchTSTFMConfig):
        super().__init__(config)

        self.config = config
        self.backbone = PatchTSTFMModel(config)

        # move all out_layer items here

    def forward(
        self,
        inputs: torch.Tensor,
        pred_mask: torch.Tensor,
        miss_mask: torch.Tensor,
        pad_mask: torch.Tensor,
        output_hidden_states: Optional[bool] = False,
        return_loss: bool = True,
        return_dict: Optional[bool] = None,
    ) -> PatchTSTFMPretrainingOutput:
        # move quantile logic here

        model_outputs = self.backbone(
            inputs,
            pred_mask=pred_mask,
            miss_mask=miss_mask,
            pad_mask=pad_mask,
            output_hidden_states=output_hidden_states,
            return_dict=True,
        )

        q_pred = model_outputs.quantile_predictions
        x_target = model_outputs.normed_target
        loss_mask = model_outputs.loss_mask

        if return_loss:
            x_target = x_target.unsqueeze(-1)
            quantiles = torch.tensor(self.backbone.quantile_levels, device=x_target.device).view(1, 1, -1)
            loss = 2 * torch.abs((x_target - q_pred) * ((x_target <= q_pred).float() - quantiles))
            loss = loss * loss_mask.unsqueeze(-1)
            loss = loss.sum(dim=1) / torch.clamp(loss_mask.sum(dim=1, keepdim=True), min=1)
            loss = loss.sum(dim=-1).mean() / math.sqrt(self.config.num_quantile)
        else:
            loss = None

        x_pred = q_pred.permute(0, 2, 1)
        x_pred = self.backbone.norm_fn.inverse_transform(x_pred)

        return PatchTSTFMPretrainingOutput(
            quantile_predictions=x_pred, loss=loss, hidden_states=model_outputs.hidden_states
        )


class PatchTSTFMForPrediction(PatchTSTFMPreTrainedModel):
    def __init__(self, config: PatchTSTFMConfig):
        super().__init__(config)

        self.config = config
        self.backbone = PatchTSTFMModel(config)

    def model_summary(self) -> str:
        return self.backbone.model_summary()

    def forward(
        self,
        inputs: List[torch.Tensor] | torch.Tensor,
        prediction_length: Optional[int] = None,
        quantile_levels: Optional[List[float]] = None,
        output_hidden_states: Optional[bool] = False,
        return_loss: bool = True,
        return_dict: Optional[bool] = None,
    ):
        forecast_len = prediction_length if prediction_length else self.config.prediction_length

        cl = self.config.context_length
        ul = -1
        logger.info(
            f"Context Len: {cl} | Forecast Len: {forecast_len} ",
        )
        cl = [cl] * len(inputs)
        fl = [
            max(
                forecast_len,
                ul,
                self.config.d_patch * max(self.config.pretrain_mask_cont, 2),
            )
        ] * len(inputs)
        forecast_samples, hidden_states = self.forecast_single_step(
            inputs, fl, context_len=cl, output_hidden_states=output_hidden_states
        )
        forecast_samples = torch.stack(forecast_samples, dim=0)[:, :, :forecast_len]

        if quantile_levels is not None:
            quantile_indices = [self.backbone.quantile_levels.index(q) for q in quantile_levels]
            forecast_samples = forecast_samples[:, quantile_indices, :]
        return PatchTSTFMPredictionOutput(quantile_predictions=forecast_samples, hidden_states=hidden_states)

    def forecast_single_step(
        self,
        x: List[torch.Tensor],
        forecast_len: List[int],
        context_len: List[int],
        output_hidden_states: Optional[bool] = False,
    ):
        """
        x: list of torch.Tensor of time series, can be of different lengths
        """

        inputs = []
        pad_mask = []
        pred_mask = []
        miss_mask = []
        ts_ends = []
        time_index = []
        sample_lengths = []

        for x_i, c_i, f_i in zip(x, context_len, forecast_len):
            c_i = min(x_i.shape[0] + f_i, c_i)
            s_i = c_i - f_i
            x_in = x_i[-s_i:]
            pad_mask_i = torch.zeros_like(x_in)
            miss_mask_i = torch.zeros_like(x_in)
            x_in = torch.nan_to_num(x_in, nan=x_in.nanmean().item())
            pred_mask_i = torch.cat([torch.zeros_like(x_in), torch.ones(f_i)], dim=-1)
            miss_mask_i = torch.cat([miss_mask_i, torch.zeros(f_i)], dim=-1)
            pad_mask_i = torch.cat([pad_mask_i, torch.zeros(f_i)], dim=-1)
            x_in = torch.cat([x_in, torch.ones(f_i) * x_in.nanmean().item()], dim=-1)
            time_index_i = (
                torch.arange(
                    self.config.context_length - x_in.shape[-1] + 1,
                    self.config.context_length + 1,
                ).float()
                / self.config.context_length
            )
            sample_len = x_in.shape[-1]
            if sample_len == self.config.context_length:
                inputs.append(x_in)
                pred_mask.append(pred_mask_i)
                pad_mask.append(pad_mask_i)
                miss_mask.append(miss_mask_i)
                time_index.append(time_index_i)
                ts_ends.append(torch.tensor([0, sample_len]).float())
                sample_lengths.append(sample_len)
            elif sample_len < self.config.context_length:  # padding
                left_pad = self.config.context_length - sample_len
                inputs.append(
                    F.pad(
                        x_in,
                        (left_pad, 0),
                        mode="constant",
                        value=x_in.nanmean().item(),
                    )
                )
                pred_mask.append(F.pad(pred_mask_i, (left_pad, 0), mode="constant", value=0.0))
                pad_mask.append(F.pad(pad_mask_i, (left_pad, 0), mode="constant", value=1.0))
                miss_mask.append(F.pad(miss_mask_i, (left_pad, 0), mode="constant", value=0.0))
                time_index.append(F.pad(time_index_i, (left_pad, 0), mode="constant", value=-1))
                ts_ends.append(torch.tensor([left_pad, left_pad + sample_len]).float())
                sample_lengths.append(sample_len)
            else:  # subsample
                inputs.append(
                    F.interpolate(
                        x_in.view(1, 1, -1),
                        size=self.config.context_length,
                        mode="nearest",
                    ).squeeze()
                )
                pred_mask.append(
                    F.interpolate(
                        pred_mask_i.view(1, 1, -1),
                        size=self.config.context_length,
                        mode="nearest",
                    ).squeeze()
                )
                pad_mask.append(
                    F.interpolate(
                        pad_mask_i.view(1, 1, -1),
                        size=self.config.context_length,
                        mode="nearest",
                    ).squeeze()
                )
                miss_mask.append(
                    F.interpolate(
                        miss_mask_i.view(1, 1, -1),
                        size=self.config.context_length,
                        mode="nearest",
                    ).squeeze()
                )
                time_index.append(
                    F.interpolate(
                        time_index_i.view(1, 1, -1),
                        size=self.config.context_length,
                        mode="nearest",
                    ).squeeze()
                )
                ts_ends.append(torch.tensor([0, self.config.context_length]).float())
                sample_lengths.append(sample_len)

        inputs = torch.stack(inputs, dim=0)
        pred_mask = torch.stack(pred_mask, dim=0)
        pad_mask = torch.stack(pad_mask, dim=0)
        miss_mask = torch.stack(miss_mask, dim=0)
        time_index = torch.stack(time_index, dim=0)
        ts_ends = torch.stack(ts_ends, dim=0)

        precision = (
            torch.bfloat16
            if torch.cuda.is_available() and torch.cuda.get_device_capability()[0] >= 8
            else torch.float16
        )
        device = "cuda" if torch.cuda.is_available() else "mps" if torch.mps.is_available() else "cpu"

        with torch.autocast(device_type=device, dtype=precision, enabled=True):
            model_output = self.backbone(
                inputs=inputs,
                pred_mask=pred_mask,
                miss_mask=miss_mask,
                pad_mask=pad_mask,
                return_loss=False,
                output_hidden_states=output_hidden_states,
            )
            outputs = model_output.quantile_predictions

        outputs = outputs.permute(0, 2, 1)
        outputs = self.backbone.norm_fn.inverse_transform(outputs)

        x_preds = []
        for i in range(outputs.shape[0]):
            if sample_lengths[i] <= self.config.context_length:
                x_pred = outputs[i][:, int(ts_ends[i][0]) : int(ts_ends[i][1])]
            else:
                x_pred = F.interpolate(outputs[i].unsqueeze(1), size=sample_lengths[i], mode="linear").squeeze(1)
            x_preds.append(x_pred[:, -forecast_len[i] :])
        return x_preds, model_output.hidden_states
