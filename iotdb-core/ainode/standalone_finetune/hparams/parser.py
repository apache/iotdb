import json
import logging
import os
from dataclasses import fields
from typing import Any, Dict, Tuple

import yaml

from standalone_finetune.config import get_exp_dir, get_models_dir
from standalone_finetune.model.model_constants import CONFIG_JSON
from standalone_finetune.model.model_info import ModelInfo
from standalone_finetune.model.utils import get_model_path
from standalone_finetune.hparams.data_args import DataArguments
from standalone_finetune.hparams.finetune_args import FinetuneArguments
from standalone_finetune.hparams.model_args import ModelArguments
from standalone_finetune.hparams.training_args import TrainingArguments

logger = logging.getLogger(__name__)
SFT_HPARAMS_DIR = "sft_hparams"

_AllArgs = Tuple[ModelArguments, DataArguments, TrainingArguments, FinetuneArguments]


def _load_file(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            if path.endswith((".yaml", ".yml")):
                return yaml.safe_load(f) or {}
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load {path}: {e}")
        return {}


def _get_sft_hparams_path(model_id: str) -> str:
    return os.path.join(get_exp_dir(), SFT_HPARAMS_DIR, f"{model_id}.yaml")


def _get_model_config_path(model_info: ModelInfo) -> str:
    return os.path.join(
        get_models_dir(),
        model_info.category.value,
        model_info.model_id,
        CONFIG_JSON,
    )


def _build_args_from_sources(cls, *sources):
    """
    Build dataclass instance from multiple dict sources.
    Priority: first source wins (leftmost has highest priority).
    Fields not found in any source use the dataclass default.
    """
    field_names = {f.name for f in fields(cls)}
    kwargs = {}
    for f_name in field_names:
        for source in sources:
            if source and f_name in source:
                kwargs[f_name] = source[f_name]
                break
    return cls(**kwargs)


def parse_args_from_dict(config: Dict[str, Any]) -> _AllArgs:
    """
    Parse arguments from a flat or nested dictionary.

    Supports flat: {"model_type": "sundial", "learning_rate": 1e-5, ...}
    and nested:    {"model": {...}, "data": {...}, "training": {...}, "finetune": {...}}
    """
    if "model" in config and isinstance(config["model"], dict):
        flat = {}
        for section in ("model", "data", "training", "finetune"):
            if section in config and isinstance(config[section], dict):
                flat.update(config[section])
        for key, val in config.items():
            if not isinstance(val, dict):
                flat[key] = val
        config = flat

    return (
        _build_args_from_sources(ModelArguments, config),
        _build_args_from_sources(DataArguments, config),
        _build_args_from_sources(TrainingArguments, config),
        _build_args_from_sources(FinetuneArguments, config),
    )


def parse_args_from_yaml(yaml_path: str) -> _AllArgs:
    """Parse arguments from a YAML configuration file."""
    return parse_args_from_dict(_load_file(yaml_path))


def parse_params_into_args(
    req_params: Dict[str, Any],
    base_model_info: ModelInfo,
) -> _AllArgs:
    """
    Parse RPC request parameters into argument objects.

    Priority chain:
        1. req_params (user-specified from RPC)
        2. default_config (model-type defaults from sft_hparams/{model_id}.yaml)
        3. model_config (from model's config.json)
        4. dataclass defaults
    """
    default_config = _load_file(_get_sft_hparams_path(base_model_info.model_id))
    model_config = _load_file(_get_model_config_path(base_model_info))

    model_args = _build_args_from_sources(
        ModelArguments, req_params, default_config, model_config
    )
    model_args.base_model_path = get_model_path(base_model_info)
    model_args.base_model_id = base_model_info.model_id
    model_args.model_id = req_params["model_id"]
    model_args.model_type = base_model_info.model_type

    data_args = _build_args_from_sources(
        DataArguments, req_params, default_config, model_config
    )
    training_args = _build_args_from_sources(
        TrainingArguments, req_params, default_config, model_config
    )
    finetune_args = _build_args_from_sources(
        FinetuneArguments, req_params, default_config, model_config
    )

    return model_args, data_args, training_args, finetune_args
