import json
import os
from dataclasses import fields
from typing import Any, Dict, Tuple

import yaml

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import CONFIG_JSON
from iotdb.ainode.core.model.model_info import ModelInfo
from iotdb.ainode.core.model.utils import get_model_path
from timecho.ainode.core.finetune.task.task_constants import SFT_HPARAMS_DIR
from timecho.ainode.core.hparams.data_args import SUPPORTED_SCALER_TYPES, DataArguments
from timecho.ainode.core.hparams.finetune_args import FinetuneArguments
from timecho.ainode.core.hparams.model_args import ModelArguments
from timecho.ainode.core.hparams.training_args import TrainingArguments

logger = Logger()

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
    return os.path.join(
        os.getcwd(),
        AINodeDescriptor().get_config().get_ain_exp_dir(),
        SFT_HPARAMS_DIR,
        f"{model_id}.yaml",
    )


def _get_model_config_path(model_info: ModelInfo) -> str:
    return os.path.join(
        os.getcwd(),
        AINodeDescriptor().get_config().get_ain_models_dir(),
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

    model_args = _build_args_from_sources(ModelArguments, default_config, model_config)
    model_args.base_model_path = get_model_path(base_model_info)
    model_args.base_model_id = base_model_info.model_id
    model_args.model_id = req_params["model_id"]
    model_args.model_type = base_model_info.model_type
    if "seq_len" in req_params:
        try:
            model_args.seq_len = int(req_params["seq_len"])
            if model_args.seq_len <= 0:
                raise ValueError("seq_len must be positive")
        except Exception:
            model_args.seq_len = 2880
            logger.warning(
                "Finetune argument 'seq_len' must be a positive integer. Defaulting to 2880."
            )
    if "input_token_len" in req_params:
        try:
            model_args.input_token_len = int(req_params["input_token_len"])
            if model_args.input_token_len <= 0:
                raise ValueError("input_token_len must be positive")
        except Exception:
            model_args.input_token_len = 16
            logger.warning(
                "Finetune argument 'input_token_len' must be a positive integer. Defaulting to 16."
            )
    if "output_token_len" in req_params:
        try:
            model_args.output_token_lens = [int(req_params["output_token_len"])]
            if model_args.output_token_len <= 0:
                raise ValueError("output_token_len must be positive")
        except Exception:
            model_args.output_token_lens = [720]
            logger.warning(
                "Finetune argument 'output_token_len' must be a positive integer. Defaulting to 720."
            )
    if "n_samples" in req_params:
        try:
            model_args.n_samples = int(req_params["n_samples"])
            if model_args.n_samples <= 0:
                raise ValueError("n_samples must be positive")
        except Exception:
            model_args.n_samples = 20
            logger.warning(
                "Finetune argument 'n_samples' must be a positive integer. Defaulting to 20."
            )

    data_args = _build_args_from_sources(DataArguments, default_config, model_config)
    data_args.dataset_type = req_params["dataset_type"]
    data_args.data_schema_list = req_params["data_schema_list"]
    if "window_step" in req_params:
        try:
            data_args.window_step = int(req_params["window_step"])
            if data_args.window_step <= 0:
                raise ValueError("window_step must be positive")
        except Exception:
            data_args.window_step = 1
            logger.warning(
                "Finetune argument 'window_step' must be an positive integer. Defaulting to 1."
            )
    if "scale" in req_params:
        try:
            data_args.scale = bool(req_params["scale"])
        except Exception:
            data_args.scale = False
            logger.warning(
                "Finetune argument 'scale' must be True or False. Defaulting to False."
            )
    if "scaler_type" in req_params:
        data_args.scaler_type = str(req_params["scaler_type"])
        if data_args.scaler_type not in SUPPORTED_SCALER_TYPES:
            data_args.scaler_type = "standard"
            logger.warning(
                f"Unsupported scaler_type '{req_params['scaler_type']}'. "
                f"Supported options: {SUPPORTED_SCALER_TYPES}. "
                "Defaulting to 'standard'."
            )

    training_args = _build_args_from_sources(
        TrainingArguments, default_config, model_config
    )
    if "num_train_epochs" in req_params:
        try:
            training_args.num_train_epochs = int(req_params["num_train_epochs"])
            if training_args.num_train_epochs <= 0:
                raise ValueError("num_train_epochs must be positive")
        except Exception:
            training_args.num_train_epochs = 5
            logger.warning(
                "Finetune argument 'num_train_epochs' must be a positive integer. Defaulting to 5."
            )
    if "iter_per_epoch" in req_params:
        try:
            training_args.iter_per_epoch = int(req_params["iter_per_epoch"])
            if (
                training_args.iter_per_epoch is not None
                and training_args.iter_per_epoch <= 0
            ):
                raise ValueError("iter_per_epoch must be positive")
        except Exception:
            training_args.iter_per_epoch = None
            logger.warning(
                "Finetune argument 'iter_per_epoch' must be a positive integer. Defaulting to None (employ all data)."
            )
    if "train_batch_size" in req_params:
        try:
            training_args.train_batch_size = int(req_params["train_batch_size"])
            if training_args.train_batch_size <= 0:
                raise ValueError("train_batch_size must be positive")
        except Exception:
            training_args.train_batch_size = 64
            logger.warning(
                "Finetune argument 'train_batch_size' must be a positive integer. Defaulting to 32."
            )
    if "val_batch_size" in req_params:
        try:
            training_args.val_batch_size = int(req_params["val_batch_size"])
            if training_args.val_batch_size <= 0:
                raise ValueError("val_batch_size must be positive")
        except Exception:
            training_args.val_batch_size = 64
            logger.warning(
                "Finetune argument 'val_batch_size' must be a positive integer. Defaulting to 64."
            )
    if "learning_rate" in req_params:
        try:
            training_args.learning_rate = float(req_params["learning_rate"])
            if training_args.learning_rate <= 0:
                raise ValueError("learning_rate must be positive")
        except Exception:
            training_args.learning_rate = 1e-5
            logger.warning(
                "Finetune argument 'learning_rate' must be a positive float. Defaulting to 1e-5."
            )
    if "weight_decay" in req_params:
        try:
            training_args.weight_decay = float(req_params["weight_decay"])
            if training_args.weight_decay < 0:
                raise ValueError("weight_decay must be non-negative")
        except Exception:
            training_args.weight_decay = 0.1
            logger.warning(
                "Finetune argument 'weight_decay' must be a non-negative float. Defaulting to 0.1."
            )
    if "schedule_type" in req_params:
        training_args.schedule_type = str(req_params["schedule_type"])
        if training_args.schedule_type not in (
            "cosine",
            "linear",
            "constant",
            "cosine_with_restarts",
            "polynomial",
            "constant_with_warmup",
        ):
            training_args.schedule_type = "cosine"
            logger.warning(
                f"Unsupported schedule_type '{req_params['schedule_type']}'. "
                "Supported options: 'cosine', 'linear', 'constant', 'cosine_with_restarts', 'polynomial', 'constant_with_warmup'. "
                "Defaulting to 'cosine'."
            )
    if "warmup_steps" in req_params:
        try:
            training_args.warmup_steps = int(req_params["warmup_steps"])
            if training_args.warmup_steps < 0:
                raise ValueError("warmup_steps must be non-negative")
        except Exception:
            training_args.warmup_steps = 1000
            logger.warning(
                "Finetune argument 'warmup_steps' must be a non-negative integer. Defaulting to 1000."
            )
    if "early_stopping_patience" in req_params:
        try:
            training_args.early_stopping_patience = int(
                req_params["early_stopping_patience"]
            )
            if training_args.early_stopping_patience < 0:
                raise ValueError("early_stopping_patience must be non-negative")
        except Exception:
            training_args.early_stopping_patience = 3
            logger.warning(
                "Finetune argument 'early_stopping_patience' must be a non-negative integer. Defaulting to 3."
            )
    if "seed" in req_params:
        try:
            training_args.seed = int(req_params["seed"])
        except Exception:
            training_args.seed = 2021
            logger.warning(
                "Finetune argument 'seed' must be an integer. Defaulting to 2021."
            )

    finetune_args = _build_args_from_sources(
        FinetuneArguments, default_config, model_config
    )
    if "finetune_type" in req_params:
        finetune_args.finetune_type = str(req_params["finetune_type"])
        if finetune_args.finetune_type not in (
            "full",
            "linear",
            "weaver_cnn",
            "weaver_mlp",
        ):
            finetune_args.finetune_type = "full"
            logger.warning(
                f"Unsupported finetune_type '{req_params['finetune_type']}'. "
                "Supported options: 'full', 'linear', 'weaver_cnn', 'weaver_mlp'. "
                "Defaulting to 'full'."
            )
        if finetune_args.finetune_type in ("weaver_cnn", "weaver_mlp"):
            finetune_args.input_channel = int(req_params["input_channel"])
            # finetune_args.output_channel = int(req_params["output_channel"])
            # finetune_args.kernel_size = int(req_params["kernel_size"])
            # finetune_args.dropout = float(req_params["dropout"])
            # finetune_args.zero_init = bool(req_params["zero_init"])
            # finetune_args.test_pred_len = int(req_params["test_pred_len"])
            # finetune_args.test_n_sample = int(req_params["test_n_sample"])

    return model_args, data_args, training_args, finetune_args


def save_args_to_yaml(
    model_args: ModelArguments,
    data_args: DataArguments,
    training_args: TrainingArguments,
    finetune_args: FinetuneArguments,
    output_path: str,
) -> None:
    """Save arguments to a YAML file."""
    config = {
        "model": model_args.to_dict(),
        "data": data_args.to_dict(),
        "training": training_args.to_dict(),
        "finetune": finetune_args.to_dict(),
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
