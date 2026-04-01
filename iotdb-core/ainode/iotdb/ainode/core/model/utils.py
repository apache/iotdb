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

import importlib
import json
import os.path
import shutil
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Type

from huggingface_hub import PyTorchModelHubMixin, snapshot_download
from transformers import (
    AutoConfig,
    AutoModelForCausalLM,
    AutoModelForNextSentencePrediction,
    AutoModelForSeq2SeqLM,
    AutoModelForSequenceClassification,
    AutoModelForTimeSeriesPrediction,
    AutoModelForTokenClassification,
    PretrainedConfig,
    PreTrainedModel,
)

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.exception import InvalidModelUriException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import (
    CONFIG_JSON,
    MODEL_SAFETENSORS,
    ModelCategory,
    UriType,
)
from iotdb.ainode.core.model.model_info import ModelInfo

logger = Logger()


def parse_uri_type(uri: str) -> UriType:
    if uri.startswith("file://"):
        return UriType.FILE
    else:
        raise InvalidModelUriException(
            f"Unknown uri type {uri}, currently supporting formats: file://"
        )


def get_parsed_uri(uri: str) -> str:
    return uri[7:]  # Remove "repo://" or "file://" prefix


@contextmanager
def temporary_sys_path(path: str):
    """Context manager for temporarily adding a path to sys.path"""
    path_added = path not in sys.path
    if path_added:
        sys.path.insert(0, path)
    try:
        yield
    finally:
        if path_added and path in sys.path:
            sys.path.remove(path)


def load_model_config_in_json(config_path: str) -> Dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_model_path(model_info: ModelInfo) -> str:
    return os.path.join(
        os.getcwd(),
        AINodeDescriptor().get_config().get_ain_models_dir(),
        model_info.category.value,
        model_info.model_id,
    )


def get_model_and_config_by_native_code(
    model_info: ModelInfo,
) -> Tuple[
    Optional[Type[PreTrainedModel | PyTorchModelHubMixin]], Optional[PretrainedConfig]
]:
    """
    Return model_class and config_instance (optionally) from the model's native code.
    """

    # Try to get model str and config str.
    config_str = None
    if model_info.auto_map:
        config_str = model_info.auto_map.get("AutoConfig", "")
        model_str = model_info.auto_map.get("AutoModelForCausalLM", "")
        if not config_str or not model_str:
            return None, None
    elif model_info.hub_mixin_cls:
        model_str = model_info.hub_mixin_cls
    else:
        return None, None

    model_path = get_model_path(model_info)

    # Try to import model and config class.
    config_class, config_instance = None, None
    model_class = None
    if model_info.category == ModelCategory.BUILTIN:
        module_name = (
            AINodeDescriptor().get_config().get_ain_models_builtin_dir()
            + "."
            + model_info.model_id
        )
        if config_str:
            # For Transformer models
            config_class = import_class_from_path(module_name, config_str)
            config_instance = config_class.from_pretrained(model_path)
        model_class = import_class_from_path(module_name, model_str)
    else:
        module_parent = str(Path(model_path).parent.absolute())
        with temporary_sys_path(module_parent):
            if config_str:
                # For Transformer models
                config_class = import_class_from_path(model_info.model_id, config_str)
                config_instance = config_class.from_pretrained(model_path)
            model_class = import_class_from_path(model_info.model_id, model_str)

    return model_class, config_instance


def get_model_and_config_by_auto_class(model_path: str) -> Tuple[type, Any]:
    """Return model_class and config_instance from Huggingface Transformers's AutoClass."""
    config_instance = AutoConfig.from_pretrained(model_path)

    if type(config_instance) in AutoModelForTimeSeriesPrediction._model_mapping.keys():
        model_class = AutoModelForTimeSeriesPrediction
    elif (
        type(config_instance)
        in AutoModelForNextSentencePrediction._model_mapping.keys()
    ):
        model_class = AutoModelForNextSentencePrediction
    elif type(config_instance) in AutoModelForSeq2SeqLM._model_mapping.keys():
        model_class = AutoModelForSeq2SeqLM
    elif (
        type(config_instance)
        in AutoModelForSequenceClassification._model_mapping.keys()
    ):
        model_class = AutoModelForSequenceClassification
    elif type(config_instance) in AutoModelForTokenClassification._model_mapping.keys():
        model_class = AutoModelForTokenClassification
    else:
        model_class = AutoModelForCausalLM

    return model_class, config_instance


def validate_model_files(model_dir: str) -> Tuple[str, str]:
    """Validate model files exist, return config and weights file paths"""

    config_path = os.path.join(model_dir, CONFIG_JSON)
    weights_path = os.path.join(model_dir, MODEL_SAFETENSORS)

    if not os.path.exists(config_path):
        raise InvalidModelUriException(
            f"Model config file does not exist: {config_path}"
        )
    if not os.path.exists(weights_path):
        raise InvalidModelUriException(
            f"Model weights file does not exist: {weights_path}"
        )

    # Create __init__.py file to ensure model directory can be imported as a module
    init_file = os.path.join(model_dir, "__init__.py")
    if not os.path.exists(init_file):
        with open(init_file, "w"):
            pass

    return config_path, weights_path


def import_class_from_path(module_name, class_path: str):
    file_name, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_name + "." + file_name)
    return getattr(module, class_name)


def ensure_init_file(dir_path: str):
    """Ensure __init__.py file exists in the given dir path"""
    init_file = os.path.join(dir_path, "__init__.py")
    os.makedirs(dir_path, exist_ok=True)
    if not os.path.exists(init_file):
        with open(init_file, "w"):
            pass


def _fetch_model_from_local(source_path: str, storage_path: str):
    logger.info(f"Copying model from local path: {source_path} -> {storage_path}")
    source_dir = Path(source_path)
    if not source_dir.exists():
        raise InvalidModelUriException(f"Source path does not exist: {source_path}")
    if not source_dir.is_dir():
        raise InvalidModelUriException(f"Source path is not a directory: {source_path}")
    storage_dir = Path(storage_path)
    if storage_dir.exists():
        shutil.rmtree(storage_dir)
    shutil.copytree(source_dir, storage_dir)


def _fetch_model_from_hf_repo(repo_id: str, storage_path: str):
    logger.info(
        f"Downloading model from HuggingFace repository: {repo_id} -> {storage_path}"
    )
    # Use snapshot_download to download entire repository (including config.json and model.safetensors)
    try:
        snapshot_download(
            repo_id=repo_id,
            local_dir=storage_path,
            local_dir_use_symlinks=False,
        )
    except Exception as e:
        logger.error(f"Failed to download model from HuggingFace: {e}")
        raise
