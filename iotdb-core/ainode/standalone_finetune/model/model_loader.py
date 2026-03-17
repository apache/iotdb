"""Load/save models for fine-tuning; uses standalone config and logging."""
import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

import torch

from standalone_finetune.model.model_constants import (
    ADAPTER_BIN,
    ADAPTER_CONFIG,
    ADAPTER_PT,
    ADAPTER_SAFETENSORS,
    CONFIG_JSON,
    MODEL_PT,
    MODEL_WEIGHT_FILES,
    TRAINING_STATE,
)
from standalone_finetune.model.model_info import ModelInfo
from standalone_finetune.model.utils import (
    get_model_and_config_by_auto_class,
    get_model_and_config_by_native_code,
    get_model_path,
)

logger = logging.getLogger(__name__)


def load_pretrained_for_finetune(
    model_info: ModelInfo,
    model_args: Any,
) -> Tuple[Any, Any]:
    """Load pretrained model and config for fine-tuning. Returns (model, config)."""
    trust_remote_code = getattr(model_args, "trust_remote_code", True)
    get_dtype = getattr(model_args, "get_torch_dtype", None)
    torch_dtype = get_dtype() if callable(get_dtype) else torch.float32
    base_model_path = getattr(model_args, "base_model_path", None) or get_model_path(model_info)

    model_class, config_instance = get_model_and_config_by_native_code(model_info)
    if model_class is None:
        model_class, config_instance = get_model_and_config_by_auto_class(base_model_path)

    for key in ("input_token_len", "output_token_lens", "seq_len", "n_samples"):
        value = getattr(model_args, key, None)
        if value is not None and hasattr(config_instance, key):
            setattr(config_instance, key, value)

    load_kwargs = {
        "config": config_instance,
        "trust_remote_code": trust_remote_code,
        "local_files_only": True,
        "low_cpu_mem_usage": getattr(model_args, "low_cpu_mem_usage", True),
    }
    if torch_dtype != "auto":
        load_kwargs["torch_dtype"] = torch_dtype

    model = model_class.from_pretrained(base_model_path, **load_kwargs)
    logger.info(f"Model loaded for finetune: {sum(p.numel() for p in model.parameters()):,} parameters")
    return model, config_instance


def unwrap_model(model: torch.nn.Module) -> torch.nn.Module:
    return model.module if hasattr(model, "module") else model


def save_model(
    model: torch.nn.Module,
    output_dir: str,
    config: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    os.makedirs(output_dir, exist_ok=True)
    model_to_save = unwrap_model(model)
    if hasattr(model_to_save, "save_pretrained"):
        model_to_save.save_pretrained(output_dir)
    else:
        torch.save(model_to_save.state_dict(), os.path.join(output_dir, MODEL_PT))
    config_path = os.path.join(output_dir, CONFIG_JSON)
    if config is not None and hasattr(config, "save_pretrained") and not os.path.exists(config_path):
        config.save_pretrained(output_dir)
    if metadata and os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.update(metadata)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    logger.info(f"Model saved to: {output_dir}")


def save_training_checkpoint(
    model: torch.nn.Module,
    output_dir: str,
    optimizer: torch.optim.Optimizer,
    scheduler: Any,
    global_step: int,
    best_val_loss: float,
) -> None:
    save_model(model, output_dir)
    state = {
        "global_step": global_step,
        "best_val_loss": best_val_loss,
        "optimizer_state_dict": optimizer.state_dict(),
        "scheduler_state_dict": scheduler.state_dict(),
    }
    torch.save(state, os.path.join(output_dir, TRAINING_STATE))


def restore_model_weights(
    model: torch.nn.Module,
    checkpoint_dir: str,
    device: Optional[torch.device] = None,
) -> bool:
    model_to_load = unwrap_model(model)
    loc = device or "cpu"
    adapter_pt = os.path.join(checkpoint_dir, ADAPTER_PT)
    if os.path.exists(adapter_pt) and hasattr(model_to_load, "feature_weaver"):
        state = torch.load(adapter_pt, map_location=loc)
        model_to_load.a.data.copy_(state["a"])
        model_to_load.b.data.copy_(state["b"])
        model_to_load.feature_weaver.load_state_dict(state["feature_weaver"])
        return True
    if hasattr(model_to_load, "peft_config"):
        for fname in (ADAPTER_SAFETENSORS, ADAPTER_BIN):
            p = os.path.join(checkpoint_dir, fname)
            if not os.path.exists(p):
                continue
            from peft import set_peft_model_state_dict
            if fname.endswith(".safetensors"):
                from safetensors.torch import load_file
                state = load_file(p, device="cpu")
            else:
                state = torch.load(p, map_location="cpu")
            set_peft_model_state_dict(model_to_load, state)
            return True
    for fname in MODEL_WEIGHT_FILES:
        p = os.path.join(checkpoint_dir, fname)
        if not os.path.exists(p):
            continue
        if fname.endswith(".safetensors"):
            from safetensors.torch import load_file
            state = load_file(p, device="cpu")
        else:
            state = torch.load(p, map_location="cpu")
        model_to_load.load_state_dict(state)
        return True
    return False


def restore_training_state(
    checkpoint_dir: str,
    optimizer: Optional[torch.optim.Optimizer] = None,
    scheduler: Optional[Any] = None,
) -> dict:
    state_file = os.path.join(checkpoint_dir, TRAINING_STATE)
    if not os.path.exists(state_file):
        return {}
    state = torch.load(state_file, map_location="cpu")
    if optimizer and "optimizer_state_dict" in state:
        optimizer.load_state_dict(state["optimizer_state_dict"])
    if scheduler and "scheduler_state_dict" in state:
        scheduler.load_state_dict(state["scheduler_state_dict"])
    return state
