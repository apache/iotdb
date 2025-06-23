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

import os
import shutil
from collections.abc import Callable

from pylru import lrucache
from torch import nn

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import (
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_MODEL_FILE_NAME,
    BuiltInModelType,
)
from ainode.core.exception import (
    BuiltInModelNotSupportError,
)
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import (
    download_built_in_model_if_necessary,
    fetch_built_in_model,
)
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.util.lock import ModelLockPool

logger = Logger()


class ModelStorage(object):
    def __init__(self):
        self._model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        if not os.path.exists(self._model_dir):
            try:
                os.makedirs(self._model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self._builtin_model_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_builtin_models_dir()
        )
        if not os.path.exists(self._builtin_model_dir):
            try:
                os.makedirs(self._builtin_model_dir)
            except PermissionError as e:
                logger.error(e)
                raise e
        self._lock_pool = ModelLockPool()
        self._model_cache = lrucache(
            AINodeDescriptor().get_config().get_ain_model_storage_cache_size()
        )

    def register_model(self, model_id: str, uri: str):
        """
        Args:
            model_id: id of model to register
            uri: network dir path or local dir path of model to register, where model.pt and config.yaml are required,
                e.g. https://huggingface.co/user/modelname/resolve/main/ or /Users/admin/Desktop/model
        Returns:
            configs: TConfigs
            attributes: str
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        # create storage dir if not exist
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        model_storage_path = os.path.join(storage_path, DEFAULT_MODEL_FILE_NAME)
        config_storage_path = os.path.join(storage_path, DEFAULT_CONFIG_FILE_NAME)
        return fetch_model_by_uri(uri, model_storage_path, config_storage_path)

    def delete_model(self, model_id: str) -> None:
        """
        Args:
            model_id: id of model to delete
        Returns:
            None
        """
        storage_path = os.path.join(self._model_dir, f"{model_id}")
        with self._lock_pool.get_lock(model_id).write_lock():
            if os.path.exists(storage_path):
                for file_name in os.listdir(storage_path):
                    self._remove_from_cache(os.path.join(storage_path, file_name))
                shutil.rmtree(storage_path)

    def _remove_from_cache(self, file_path: str) -> None:
        if file_path in self._model_cache:
            del self._model_cache[file_path]

    def load_model(
        self, model_id: str, is_built_in: bool, acceleration: bool
    ) -> Callable:
        """
        Load a model with automatic detection of .safetensors or .pt format

        Returns:
            model: The model instance corresponding to specific model_id
        """
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
                raise NotImplementedError

    def save_model(self, model_id: str, is_built_in: bool, model: nn.Module):
        """
        Save the model using save_pretrained

        Returns:
            Whether saving succeeded
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            if is_built_in:
                if model_id not in BuiltInModelType.values():
                    raise BuiltInModelNotSupportError(model_id)
                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
                model.save_pretrained(model_dir)
            else:
                # TODO: support save the user-defined model
                # model_dir = os.path.join(self._model_dir, f"{model_id}")
                raise NotImplementedError

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        # Only support built-in models for now
        return os.path.join(self._builtin_model_dir, f"{model_id}")
