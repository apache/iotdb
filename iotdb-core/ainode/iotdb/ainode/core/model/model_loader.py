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

import json
import os
from typing import Any, Dict, Optional, Tuple

import torch

from iotdb.ainode.core.exception import ModelNotExistException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.device_manager import DeviceManager
from iotdb.ainode.core.model.model_constants import (
    ADAPTER_BIN,
    ADAPTER_CONFIG,
    ADAPTER_PT,
    ADAPTER_SAFETENSORS,
    CONFIG_JSON,
    MODEL_PT,
    MODEL_WEIGHT_FILES,
    TRAINING_STATE,
    ModelCategory,
)
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.sktime.modeling_sktime import create_sktime_model
from iotdb.ainode.core.model.utils import (
    get_model_and_config_by_auto_class,
    get_model_and_config_by_base_model,
    get_model_and_config_by_native_code,
    get_model_path,
    has_base_weights,
    search_model_path,
)
from timecho.ainode.core.hparams.model_args import ModelArguments

logger = Logger()
BACKEND = DeviceManager()


def load_model(model_info: ModelInfo, **model_kwargs) -> Any:
    if model_info.auto_map is not None:
        model = _load_transformers_model(model_info, **model_kwargs)
    elif model_info.hub_mixin_cls is not None:
        model = _load_hub_mixin_model(model_info, **model_kwargs)
    else:
        if model_info.model_type == "sktime":
            model = create_sktime_model(model_info.model_id)
        else:
            model = _load_torchscript_model(model_info, **model_kwargs)

    logger.info(
        f"Model {model_info.model_id} loaded to device {next(model.parameters()).device if model_info.model_type != 'sktime' else 'cpu'} successfully."
    )
    return model


def _load_transformers_model(model_info: ModelInfo, **model_kwargs):
    device_map = model_kwargs.get("device_map", "cpu")
    trust_remote_code = model_kwargs.get("trust_remote_code", True)
    train_from_scratch = model_kwargs.get("train_from_scratch", False)

    model_path = get_model_path(model_info)
    is_finetuned = (
        model_info.category == ModelCategory.FINE_TUNED
        and model_info.base_model_id is not None
    )

    if is_finetuned:
        model_class, config_instance = get_model_and_config_by_base_model(model_info)
    else:
        model_class, config_instance = get_model_and_config_by_native_code(model_info)
    if model_class is None:
        model_class, config_instance = get_model_and_config_by_auto_class(model_path)

    # ---- Load base model ----
    if train_from_scratch:
        model = model_class.from_config(
            config_instance, trust_remote_code=trust_remote_code
        )
    else:
        weights_path = model_path
        if is_finetuned and not has_base_weights(model_path):
            base_model_path = search_model_path(model_info.base_model_id)
            if base_model_path is not None:
                weights_path = base_model_path
            else:
                logger.warning(
                    f"Base model path not found for '{model_info.base_model_id}', "
                    f"trying fine-tuned dir: {model_path}"
                )
        model = model_class.from_pretrained(
            weights_path,
            config=config_instance,
            trust_remote_code=trust_remote_code,
        )

    # Apply adapter if present (DualWeaver / LoRA / etc.)
    if is_finetuned:
        model = _apply_adapter(model, model_path)

    return BACKEND.move_model(model, device_map)


def _load_hub_mixin_model(model_info: ModelInfo, **model_kwargs):
    device_map = model_kwargs.get("device_map", "cpu")
    model_path = get_model_path(model_info)
    model_class, _ = get_model_and_config_by_native_code(model_info)
    if model_class is None:
        logger.error(f"Model class not found for '{model_info.model_id}'")
        raise ModelNotExistException(model_info.model_id)
    # Load model
    model = model_class.from_pretrained(model_path)
    return BACKEND.move_model(model, device_map)


def _load_torchscript_model(model_info: ModelInfo, **kwargs):
    device_map = kwargs.get("device_map", "cpu")
    acceleration = kwargs.get("acceleration", False)
    model_path = get_model_path(model_info)
    model_file = os.path.join(model_path, MODEL_PT)
    if not os.path.exists(model_file):
        logger.error(f"Model file not found at {model_file}.")
        raise ModelNotExistException(model_file)
    model = torch.jit.load(model_file)
    if isinstance(model, torch._dynamo.eval_frame.OptimizedModule) or not acceleration:
        return model
    try:
        model = torch.compile(model)
    except Exception as e:
        logger.warning(f"acceleration failed, fallback to normal mode: {str(e)}")
    return BACKEND.move_model(model, device_map)


def _apply_adapter(
    model: torch.nn.Module,
    model_dir: str,
) -> torch.nn.Module:
    """Detect and apply an adapter from model_dir onto model.

    Detection is based on ``adapter_config.json``:

    "adapter_type": "dual_weaver" → :class:`DualWeaver.from_pretrained`
    "adapter_type": "peft"        → :class:`peft.PeftModel.from_pretrained`

    Returns model unchanged if no adapter config is found.
    """
    adapter_config_path = os.path.join(model_dir, ADAPTER_CONFIG)
    if not os.path.exists(adapter_config_path):
        return model

    with open(adapter_config_path, "r", encoding="utf-8") as f:
        adapter_cfg = json.load(f)

    # Normalize adapter type: our own configs use "adapter_type",
    # while the peft library writes "peft_type" instead.
    adapter_type = adapter_cfg.get("adapter_type", "")
    if not adapter_type and "peft_type" in adapter_cfg:
        adapter_type = "peft"

    if adapter_type == "dual_weaver":
        from timecho.ainode.core.finetune.adapter.weaver.dual_weaver import DualWeaver

        model = DualWeaver.from_pretrained(model_dir, ltm=model)
        logger.info(f"Loaded DualWeaver adapter from: {model_dir}")
        return model

    elif adapter_type == "peft":
        from peft import PeftModel

        model = PeftModel.from_pretrained(model, model_dir)
        logger.info(
            f"Loaded PEFT/{adapter_cfg.get('peft_type', '?')} adapter from: {model_dir}"
        )
        return model

    else:
        logger.warning(
            f"Unknown adapter format in {adapter_config_path}, using base model only"
        )
        return model


def load_pretrained_for_finetune(
    model_info: ModelInfo,
    model_args: ModelArguments,
) -> Tuple[Any, Any]:
    """
    Load a pretrained model and config for fine-tuning.

    Returns:
        Tuple of (model, config)
    """
    trust_remote_code = model_args.trust_remote_code
    torch_dtype = model_args.get_torch_dtype()

    model_path = get_model_path(model_info)
    model_class, config_instance = get_model_and_config_by_native_code(model_info)
    # TODO: Handle the case where config_instance is None (e.g. hub mixin models).
    if model_class is None:
        model_class, config_instance = get_model_and_config_by_auto_class(model_path)

    for key in ("input_token_len", "output_token_lens", "seq_len", "n_samples"):
        value = getattr(model_args, key, None)
        if value is not None and hasattr(config_instance, key):
            setattr(config_instance, key, value)

    load_kwargs = {
        "config": config_instance,
        "trust_remote_code": trust_remote_code,
        "local_files_only": True,
        "low_cpu_mem_usage": model_args.low_cpu_mem_usage,
    }
    if torch_dtype != "auto":
        load_kwargs["dtype"] = torch_dtype

    model = model_class.from_pretrained(model_args.base_model_path, **load_kwargs)

    num_params = sum(p.numel() for p in model.parameters())
    logger.info(f"Model loaded for finetune: {num_params:,} parameters")

    return model, config_instance


def unwrap_model(model: torch.nn.Module) -> torch.nn.Module:
    """Unwrap DDP / DeepSpeed / FSDP wrapper to get the underlying model."""
    return model.module if hasattr(model, "module") else model


def save_model(
    model: torch.nn.Module,
    output_dir: str,
    config: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    """Save model weights and config to output_dir.

    Automatically detects the model type and dispatches:

    * DualWeaver Model  – saves only adapter weights (adapter_config.json + adapter_model.pt).
    * LoRA/PEFT Model   – saves only adapter weights (adapter_config.json + adapter_model.safetensors).
    * HuggingFace Model – saves config.json + model.safetensors.
    * Other Plain Model – falls back to torch.save(state_dict).

    Args:
        model: Model to save (may be wrapped by DDP).
        output_dir: Target directory.
        config: HuggingFace config object.
        metadata: Extra key-value pairs merged into config.json.
    """
    os.makedirs(output_dir, exist_ok=True)
    model_to_save = unwrap_model(model)

    # 1. Dispatch model weight saving
    if hasattr(model_to_save, "save_pretrained"):
        model_to_save.save_pretrained(output_dir)
    else:
        torch.save(
            model_to_save.state_dict(),
            os.path.join(output_dir, MODEL_PT),
        )

    # 2. Ensure config.json exists (adapter-only saves like LoRA / DualWeaver
    #    only write adapter_config.json, not config.json).
    config_path = os.path.join(output_dir, CONFIG_JSON)
    if (
        not os.path.exists(config_path)
        and config is not None
        and hasattr(config, "save_pretrained")
    ):
        config.save_pretrained(output_dir)

    # 3. Merge extra metadata into config.json
    if metadata and os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config_data = json.load(f)
        config_data.update(metadata)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=2)

    logger.info(f"Model saved to: {output_dir}")


def save_training_checkpoint(
    model: torch.nn.Module,
    output_dir: str,
    optimizer: torch.optim.Optimizer,
    scheduler: Any,
    global_step: int,
    best_val_loss: float,
) -> None:
    """Save model weights and training state for mid-training checkpointing."""
    save_model(model, output_dir)
    training_state = {
        "global_step": global_step,
        "best_val_loss": best_val_loss,
        "optimizer_state_dict": optimizer.state_dict(),
        "scheduler_state_dict": scheduler.state_dict(),
    }
    torch.save(training_state, os.path.join(output_dir, TRAINING_STATE))
    logger.info(f"Saved checkpoint: {output_dir}")


def restore_model_weights(
    model: torch.nn.Module,
    checkpoint_dir: str,
    device: Optional[torch.device] = None,
) -> bool:
    """Restore weights from *checkpoint_dir* into an existing *model*.

    Detection order:
    1. ``adapter_model.pt`` + ``feature_weaver`` attr     → DualWeaver adapter
    2. ``adapter_model.safetensors`` / ``.bin`` + ``peft_config`` → PEFT / LoRA adapter
    3. ``model.safetensors`` → full model (safetensors)
    4. ``model.pt``          → full model (PyTorch state_dict)
    5. ``pytorch_model.bin`` → full model (legacy format)

    Returns:
        ``True`` if weights were successfully restored.
    """
    model_to_load = unwrap_model(model)

    # ---- DualWeaver adapter (a, b, feature_weaver) ----
    adapter_path = os.path.join(checkpoint_dir, ADAPTER_PT)
    if os.path.exists(adapter_path) and hasattr(model_to_load, "feature_weaver"):
        adapter_state = torch.load(adapter_path, map_location=device or "cpu")
        model_to_load.a.data.copy_(adapter_state["a"])
        model_to_load.b.data.copy_(adapter_state["b"])
        model_to_load.feature_weaver.load_state_dict(adapter_state["feature_weaver"])
        logger.info(f"Restored DualWeaver adapter from: {adapter_path}")
        return True

    # ---- PEFT / LoRA adapter ----
    if hasattr(model_to_load, "peft_config"):
        for fname in (ADAPTER_SAFETENSORS, ADAPTER_BIN):
            peft_path = os.path.join(checkpoint_dir, fname)
            if not os.path.exists(peft_path):
                continue
            if fname.endswith(".safetensors"):
                from safetensors.torch import load_file

                adapter_state = load_file(peft_path, device="cpu")
            else:
                adapter_state = torch.load(peft_path, map_location="cpu")
            from peft import set_peft_model_state_dict

            set_peft_model_state_dict(model_to_load, adapter_state)
            logger.info(f"Restored PEFT adapter from: {peft_path}")
            return True

    # ---- Full model weights ----
    for fname in MODEL_WEIGHT_FILES:
        weight_path = os.path.join(checkpoint_dir, fname)
        if not os.path.exists(weight_path):
            continue
        if fname.endswith(".safetensors"):
            from safetensors.torch import load_file

            state_dict = load_file(weight_path, device="cpu")
        else:
            state_dict = torch.load(weight_path, map_location="cpu")
        model_to_load.load_state_dict(state_dict)
        logger.info(f"Restored model weights from: {weight_path}")
        return True

    logger.warning(f"No model weights found in {checkpoint_dir}")
    return False


def restore_training_state(
    checkpoint_dir: str,
    optimizer: Optional[torch.optim.Optimizer] = None,
    scheduler: Optional[Any] = None,
) -> dict:
    """Restore optimizer, scheduler and step counters from a checkpoint.

    Args:
        checkpoint_dir: Directory containing ``training_state.pt``.
        optimizer: If provided, its state is restored from the checkpoint.
        scheduler: If provided, its state is restored from the checkpoint.

    Returns:
        Dict with keys such as ``global_step``, ``best_val_loss``, etc.
        Returns an empty dict if ``training_state.pt`` does not exist.
    """
    state_file = os.path.join(checkpoint_dir, TRAINING_STATE)
    if not os.path.exists(state_file):
        return {}

    state = torch.load(state_file, map_location="cpu")

    if optimizer is not None and "optimizer_state_dict" in state:
        optimizer.load_state_dict(state["optimizer_state_dict"])
    if scheduler is not None and "scheduler_state_dict" in state:
        scheduler.load_state_dict(state["scheduler_state_dict"])

    logger.info(f"Loaded training state from: {state_file}")
    return state
