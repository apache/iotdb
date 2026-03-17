"""Model path and class resolution; uses standalone config."""
import importlib
import os
import sys
from pathlib import Path
from typing import Any, Optional, Tuple

from transformers import AutoConfig, AutoModelForCausalLM

from standalone_finetune.config import get_builtin_module, get_models_dir
from standalone_finetune.model.model_constants import CONFIG_JSON, ModelCategory
from standalone_finetune.model.model_info import BUILTIN_HF_TRANSFORMERS_MODEL_MAP, ModelInfo


def get_model_path(model_info: ModelInfo) -> str:
    return os.path.join(get_models_dir(), model_info.category.value, model_info.model_id)


def search_model_path(model_id: str) -> Optional[str]:
    for category in ModelCategory:
        candidate = os.path.join(get_models_dir(), category.value, model_id)
        if os.path.isdir(candidate):
            return candidate
    return None


def _import_class_from_path(module_name: str, class_path: str) -> type:
    file_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name + "." + file_name)
    return getattr(module, class_name)


def get_model_and_config_by_native_code(model_info: ModelInfo) -> Tuple[Optional[type], Optional[Any]]:
    if not model_info.auto_map:
        return None, None
    config_str = model_info.auto_map.get("AutoConfig", "")
    model_str = model_info.auto_map.get("AutoModelForCausalLM", "")
    if not config_str or not model_str:
        return None, None
    model_path = get_model_path(model_info)
    builtin_prefix = get_builtin_module()
    if model_info.category == ModelCategory.BUILTIN:
        module_name = f"{builtin_prefix}.{model_info.model_id}"
    else:
        module_name = model_info.model_id
        parent = str(Path(model_path).parent.absolute())
        if parent not in sys.path:
            sys.path.insert(0, parent)
    try:
        config_class = _import_class_from_path(module_name, config_str)
        model_class = _import_class_from_path(module_name, model_str)
        config_instance = config_class.from_pretrained(model_path)
        return model_class, config_instance
    except Exception:
        return None, None


def get_model_and_config_by_auto_class(model_path: str) -> Tuple[type, Any]:
    config_instance = AutoConfig.from_pretrained(model_path)
    return AutoModelForCausalLM, config_instance
