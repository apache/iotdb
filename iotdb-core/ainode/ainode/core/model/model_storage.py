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
import shutil
from collections.abc import Callable
from pathlib import Path

import torch
import torch._dynamo
from pylru import lrucache

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import (
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_MODEL_FILE_NAME,
    JSON_CONFIG_FILES,
    WEIGHT_FORMAT_PRIORITY, BuiltInModelType,
)
from ainode.core.exception import ModelLoadingError, ModelNotExistError, BuiltInModelNotSupportError
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import download_built_in_model_if_necessary, fetch_built_in_model
from ainode.core.model.config_parser import (
    convert_iotdb_config_to_ainode_format,
    detect_config_format,
    parse_config_file,
    validate_iotdb_config,
)
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.model.safetensor_loader import (
    get_available_weight_files,
    load_weights_as_state_dict,
    load_weights_for_from_pretrained,
)
from ainode.core.util.lock import ModelLockPool

logger = Logger()


class ModelStorage(object):
    def __init__(self):
        self._model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        self._builtin_model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        if not os.path.exists(self._model_dir):
            try:
                os.makedirs(self._model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self._lock_pool = ModelLockPool()
        self._model_cache = lrucache(
            AINodeDescriptor().get_config().get_ain_model_storage_cache_size()
        )

    def register_model(self, model_id: str, uri: str):
        """
        Register a model from a given URI

        Args:
            model_id: ID of the model to register
            uri: Network or local directory path where the model files are located

        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        # Create storage directory if it does not exist
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)

        # Use default file names; actual format is auto-detected by the model factory
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)

        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)

    def _load_iotdb_model(
        self, model_dir: str, config_file: str, weight_file: str, acceleration: bool
    ) -> Callable:
        """
        Load a model in IoTDB format with config_parser and safetensor_loader integration
        """
        config_path = os.path.join(model_dir, config_file)
        weight_path = os.path.join(model_dir, weight_file)

        # Use composite key for caching
        cache_key = f"{config_path}:{weight_path}"

        if cache_key in self._model_cache:
            model = self._model_cache[cache_key]
            if (
                isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
                or not acceleration
            ):
                return model
            else:
                model = torch.compile(model)
                self._model_cache[cache_key] = model
                return model

        try:
            config_dict = parse_config_file(config_path)

            if not validate_iotdb_config(config_dict):
                raise ValueError(f"Invalid IoTDB configuration: {config_path}")

            model_type = config_dict.get("model_type", "unknown")
            logger.info(f"Loading IoTDB model: {model_type}")

            try:
                weights = load_weights_for_from_pretrained(model_dir, weight_file)
                logger.debug(
                    f"Weight file validated successfully with {len(weights)} parameters"
                )
            except Exception as e:
                logger.error(f"Weight file validation failed: {e}")
                raise ModelNotExistError(
                    f"Corrupted or incompatible weight file: {weight_path}"
                )

            if model_type == "timer":
                from ainode.core.model.timerxl.modeling_timer import (
                    TimerForPrediction as ModelClass,
                )
            elif model_type == "sundial":
                from ainode.core.model.sundial.modeling_sundial import (
                    SundialForPrediction as ModelClass,
                )
            else:
                raise ValueError(f"Unsupported model type: {model_type}")

            model = ModelClass.from_pretrained(model_dir)
            model.eval()

            try:
                input_length = config_dict.get("input_token_len", 96)
                example_input = torch.randn(1, input_length)
                traced_model = torch.jit.trace(model, example_input)
                logger.debug(
                    f"Successfully converted IoTDB model to TorchScript: {model_type}"
                )
                model = traced_model
            except Exception as e:
                logger.warning(
                    f"TorchScript conversion failed, using original model: {e}"
                )

            if acceleration:
                try:
                    model = torch.compile(model)
                    logger.debug(f"Model acceleration enabled: {model_type}")
                except Exception as e:
                    logger.warning(f"Model acceleration failed: {e}")

            self._model_cache[cache_key] = model
            return model

        except Exception as e:
            logger.error(f"Failed to load IoTDB model: {e}")
            raise ModelNotExistError(f"Unable to load model: {weight_path}")

    def _load_legacy_model(self, model_path: str, acceleration: bool) -> Callable:
        """
        Load model in legacy format
        """
        if model_path in self._model_cache:
            model = self._model_cache[model_path]
            if (
                isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
                or not acceleration
            ):
                return model
            else:
                model = torch.compile(model)
                self._model_cache[model_path] = model
                return model
        else:
            if not os.path.exists(model_path):
                raise ModelNotExistError(model_path)

            model = torch.jit.load(model_path)
            if acceleration:
                try:
                    model = torch.compile(model)
                except Exception as e:
                    logger.warning(
                        f"Acceleration failed, falling back to normal mode: {str(e)}"
                    )

            self._model_cache[model_path] = model
            return model

    def load_model(self, model_id: str, is_built_in: bool, acceleration: bool) -> Callable:
        """
        Load a model with automatic detection of .safetensors or .pt format

        Returns:
            model: The model instance corresponding to specific model_id
        """
        model_id = model_id.lower()
        with self._lock_pool.get_lock(model_id).read_lock():
            if is_built_in:
                if model_id not in BuiltInModelType.values():
                    raise BuiltInModelNotSupportError(model_id)
                # For built-in models, we support auto download
                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
                download_built_in_model_if_necessary(model_id, model_dir)
                return fetch_built_in_model(model_id, model_dir)
            else:
                # TODO: support load the user-defined model
                # model_dir = os.path.join(self._model_dir, f"{model_id}")
                pass


    def delete_model(self, model_id: str) -> None:
        """
        Delete the model and remove it from cache

        Args:
            model_id: ID of the model to delete

        Returns:
            None
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        with self._lock_pool.get_lock(model_id).write_lock():
            if os.path.exists(storage_path):
                keys_to_remove = []
                for cache_key in list(self._model_cache.keys()):
                    if storage_path in str(cache_key):
                        keys_to_remove.append(cache_key)

                for key in keys_to_remove:
                    del self._model_cache[key]
                for file_name in os.listdir(storage_path):
                    self._remove_from_cache(os.path.join(storage_path, file_name))
                shutil.rmtree(storage_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self._model_cache:
            del self._model_cache[file_path]

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        return os.path.join(self._model_dir, f"{model_id}")

    def validate_model_files(self, model_id: str) -> dict:
        """
        Validate the integrity of model files

        Args:
            model_id: Model ID

        Returns:
            Dictionary containing validation results
        """
        model_dir = os.path.join(self._model_dir, f"{model_id}")
        result = {
            "valid": False,
            "format": None,
            "config_file": None,
            "weight_file": None,
            "available_files": {},
            "errors": [],
        }

        try:
            # Detect format
            format_type, config_file, weight_file = self._detect_local_model_format(
                model_dir
            )
            result["format"] = format_type
            result["config_file"] = config_file
            result["weight_file"] = weight_file

            # Get available files
            result["available_files"] = get_available_weight_files(model_dir)

            if format_type == "iotdb":
                # Validate IoTDB format
                config_path = os.path.join(model_dir, config_file)
                weight_path = os.path.join(model_dir, weight_file)

                # Validate config file
                config_dict = parse_config_file(config_path)
                if not validate_iotdb_config(config_dict):
                    result["errors"].append("Configuration file validation failed")

                # Validate weight file
                try:
                    weights = load_weights_for_from_pretrained(model_dir, weight_file)
                    logger.debug(f"Weight file contains {len(weights)} parameters")
                    result["valid"] = True
                except Exception as e:
                    result["errors"].append(f"Weight file validation failed: {e}")

            elif format_type == "legacy":
                result["valid"] = True
            else:
                result["errors"].append("Unrecognized model format")

        except Exception as e:
            result["errors"].append(f"Validation error: {e}")

        return result

    def _load_iotdb_model_with_from_pretrained(
        self, model_dir: str, config_file: str, weight_file: str, acceleration: bool
    ) -> Callable:
        """
        Full from_pretrained-based loading for IoTDB model format
        """
        config_path = os.path.join(model_dir, config_file)
        weight_path = os.path.join(model_dir, weight_file)

        # Use composite cache key
        cache_key = f"iotdb:{config_path}:{weight_path}"

        if cache_key in self._model_cache:
            model = self._model_cache[cache_key]
            if (
                isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
                or not acceleration
            ):
                return model
            else:
                model = torch.compile(model)
                self._model_cache[cache_key] = model
                return model

        try:
            config_dict = parse_config_file(config_path)
            model_type = config_dict.get("model_type", "unknown")

            logger.info(f"Loading IoTDB model with from_pretrained: {model_type}")

            # Dynamically import model class
            if model_type == "timer":
                from ainode.core.model.timerxl.configuration_timer import (
                    TimerConfig as ConfigClass,
                )
                from ainode.core.model.timerxl.modeling_timer import (
                    TimerForPrediction as ModelClass,
                )
            elif model_type == "sundial":
                from ainode.core.model.sundial.configuration_sundial import (
                    SundialConfig as ConfigClass,
                )
                from ainode.core.model.sundial.modeling_sundial import (
                    SundialForPrediction as ModelClass,
                )
            else:
                raise ValueError(f"Unsupported model type: {model_type}")

            model_config = ConfigClass.from_dict(config_dict)
            state_dict = load_weights_for_from_pretrained(model_dir, weight_file)

            model = ModelClass.from_pretrained(
                pretrained_model_name_or_path=model_dir,
                config=model_config,
                state_dict=state_dict,
                torch_dtype=torch.float32,
            )

            model.eval()
            logger.info(f"Successfully loaded model {model_type} with from_pretrained")

            if self._should_convert_to_torchscript(config_dict):
                try:
                    input_length = config_dict.get("input_token_len", 96)
                    example_input = torch.randn(1, input_length)
                    traced_model = torch.jit.trace(model, example_input)
                    logger.debug(f"Converted to TorchScript: {model_type}")
                    model = traced_model
                except Exception as e:
                    logger.warning(f"TorchScript conversion failed: {e}")

            if acceleration:
                try:
                    model = torch.compile(model)
                    logger.debug(f"Applied acceleration: {model_type}")
                except Exception as e:
                    logger.warning(f"Acceleration failed: {e}")

            self._model_cache[cache_key] = model
            return model

        except Exception as e:
            logger.error(f"from_pretrained failed for {model_type}: {e}")
            raise ModelLoadingError(model_type, str(e))

    def save_model_with_save_pretrained(
        self, model_id: str, model_obj, save_directory: str = None
    ) -> bool:
        """
        Save the model using save_pretrained

        Returns:
            Whether saving succeeded
        """
        try:
            if save_directory is None:
                save_directory = os.path.join(self._model_dir, f"{model_id}_saved")

            os.makedirs(save_directory, exist_ok=True)

            if not hasattr(model_obj, "save_pretrained"):
                logger.warning(f"Model {model_id} does not support save_pretrained")
                return False

            logger.info(f"Saving model {model_id} to {save_directory}")

            model_obj.save_pretrained(
                save_directory=save_directory,
                safe_serialization=True,  # Use safetensors format
                save_config=True,
            )

            config_file = os.path.join(save_directory, "config.json")
            weight_file = os.path.join(save_directory, "model.safetensors")

            if os.path.exists(config_file) and os.path.exists(weight_file):
                logger.info(f"Model {model_id} saved successfully")
                return True
            else:
                logger.error(f"Save verification failed for {model_id}")
                return False

        except Exception as e:
            logger.error(f"save_pretrained failed for {model_id}: {e}")
            return False

    def _should_convert_to_torchscript(self, config_dict: dict) -> bool:
        """
        Determine whether the model should be converted to TorchScript
        """
        model_type = config_dict.get("model_type", "")
        unsupported_types = [
            "sundial"
        ]  # Sundial diffusion models may not work with TorchScript
        return model_type not in unsupported_types

    def clone_model_with_save_load(
        self, source_model_id: str, target_model_id: str
    ) -> bool:
        """
        Clone a model using save_pretrained and from_pretrained
        """
        try:
            source_model = self.load_model(source_model_id, acceleration=False)

            temp_dir = os.path.join(self._model_dir, f"temp_{target_model_id}")
            if self.save_model_with_save_pretrained(
                source_model_id, source_model, temp_dir
            ):
                target_dir = os.path.join(self._model_dir, target_model_id)
                shutil.move(temp_dir, target_dir)

                logger.info(f"Model cloned: {source_model_id} -> {target_model_id}")
                return True
            else:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                return False

        except Exception as e:
            logger.error(f"Model cloning failed: {e}")
            return False
