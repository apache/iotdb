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

import concurrent.futures
import json
import os
import shutil
from collections.abc import Callable
from typing import Dict

from torch import nn

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import (
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_MODEL_FILE_NAME,
    MODEL_CONFIG_FILE_IN_JSON,
    TSStatusCode,
)
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import (
    download_ltsm_if_necessary,
    fetch_built_in_model,
)
from ainode.core.model.model_factory import fetch_model_by_uri
from ainode.core.model.model_info import (
    BUILT_IN_LTSM_MAP,
    BUILT_IN_MACHINE_LEARNING_MODEL_MAP,
    BuiltInModelType,
    ModelCategory,
    ModelInfo,
    ModelStates,
    get_built_in_model_type,
)
from ainode.core.util.lock import ModelLockPool
from ainode.thrift.ainode.ttypes import TShowModelsResp
from ainode.thrift.common.ttypes import TSStatus

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
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1
        )  # TODO: Here we set the work_num=1 cause we found that the hf download interface is not stable for concurrent downloading.
        self._model_info_map: Dict[str, ModelInfo] = {}
        self._init_model_info_map()

    def _init_model_info_map(self):
        """
        Initialize the model info map.
        """
        # 1. initialize built-in and ready-to-use models
        for model_id in BUILT_IN_MACHINE_LEARNING_MODEL_MAP:
            self._model_info_map[model_id] = BUILT_IN_MACHINE_LEARNING_MODEL_MAP[
                model_id
            ]
        # 2. retrieve fine-tuned models from the built-in model directory
        fine_tuned_models = self._retrieve_fine_tuned_models()
        for model_id in fine_tuned_models:
            self._model_info_map[model_id] = fine_tuned_models[model_id]
        # 3. automatically downloading the weights of built-in LSTM models when necessary
        for model_id in BUILT_IN_LTSM_MAP:
            if model_id not in self._model_info_map:
                self._model_info_map[model_id] = BUILT_IN_LTSM_MAP[model_id]
            future = self._executor.submit(
                self._download_built_in_model_if_necessary, model_id
            )
            future.add_done_callback(
                lambda f, mid=model_id: self._callback_model_download_result(f, mid)
            )
        # TODO: retrieve user-defined models

    def _retrieve_fine_tuned_models(self):
        """
        Retrieve fine-tuned models from the built-in model directory.

        Returns:
            {"model_id": ModelInfo}
        """
        result = {}
        build_in_dirs = [
            d
            for d in os.listdir(self._builtin_model_dir)
            if os.path.isdir(os.path.join(self._builtin_model_dir, d))
        ]
        for model_id in build_in_dirs:
            config_file_path = os.path.join(
                self._builtin_model_dir, model_id, MODEL_CONFIG_FILE_IN_JSON
            )
            if os.path.isfile(config_file_path):
                with open(config_file_path, "r") as f:
                    model_config = json.load(f)
                if "model_type" in model_config:
                    model_type = model_config["model_type"]
                    model_info = ModelInfo(
                        model_id=model_id,
                        model_type=model_type,
                        category=ModelCategory.FINE_TUNED,
                        state=ModelStates.ACTIVE,
                    )
                    # Refactor the built-in model category
                    if "timer_xl" == model_id:
                        model_info.category = ModelCategory.BUILT_IN
                    if "sundial" == model_id:
                        model_info.category = ModelCategory.BUILT_IN
                    # Compatible patch with the codes in HuggingFace
                    if "timer" == model_type:
                        model_info.model_type = BuiltInModelType.TIMER_XL.value
                    if "sundial" == model_type:
                        model_info.model_type = BuiltInModelType.SUNDIAL.value
                    result[model_id] = model_info
        return result

    def _download_built_in_model_if_necessary(self, model_id: str) -> bool:
        """
        Download the built-in model if it is not already downloaded.

        Args:
            model_id (str): The ID of the model to download.

        Return:
            bool: True if the model is existed or downloaded successfully, False otherwise.
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            local_dir = os.path.join(self._builtin_model_dir, model_id)
            return download_ltsm_if_necessary(
                get_built_in_model_type(self._model_info_map[model_id].model_type),
                local_dir,
            )

    def _callback_model_download_result(self, future, model_id: str):
        with self._lock_pool.get_lock(model_id).write_lock():
            if future.result():
                self._model_info_map[model_id].state = ModelStates.ACTIVE
                logger.info(
                    f"The built-in model: {model_id} is active and ready to use."
                )
            else:
                self._model_info_map[model_id].state = ModelStates.INACTIVE

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
                shutil.rmtree(storage_path)
        storage_path = os.path.join(self._builtin_model_dir, f"{model_id}")
        with self._lock_pool.get_lock(model_id).write_lock():
            if os.path.exists(storage_path):
                shutil.rmtree(storage_path)
            if model_id in self._model_info_map:
                del self._model_info_map[model_id]
                logger.info(f"Model {model_id} deleted successfully.")

    def _is_built_in(self, model_id: str) -> bool:
        """
        Check if the model_id corresponds to a built-in model.

        Args:
            model_id (str): The ID of the model.

        Returns:
            bool: True if the model is built-in, False otherwise.
        """
        return model_id in self._model_info_map and (
            self._model_info_map[model_id].category == ModelCategory.BUILT_IN
            or self._model_info_map[model_id].category == ModelCategory.FINE_TUNED
        )

    def load_model(self, model_id: str, acceleration: bool) -> Callable:
        """
        Load a model with automatic detection of .safetensors or .pt format

        Returns:
            model: The model instance corresponding to specific model_id
        """
        with self._lock_pool.get_lock(model_id).read_lock():
            if self._is_built_in(model_id):
                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
                return fetch_built_in_model(
                    get_built_in_model_type(self._model_info_map[model_id].model_type),
                    model_dir,
                )
            else:
                # TODO: support load the user-defined model
                # model_dir = os.path.join(self._model_dir, f"{model_id}")
                raise NotImplementedError

    def save_model(self, model_id: str, model: nn.Module):
        """
        Save the model using save_pretrained

        Returns:
            Whether saving succeeded
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            if self._is_built_in(model_id):
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

    def show_models(self) -> TShowModelsResp:
        return TShowModelsResp(
            status=TSStatus(
                code=TSStatusCode.SUCCESS_STATUS.value,
                message="Show models successfully",
            ),
            modelIdList=list(self._model_info_map.keys()),
            modelTypeMap=dict(
                (model_id, model_info.model_type)
                for model_id, model_info in self._model_info_map.items()
            ),
            categoryMap=dict(
                (model_id, model_info.category.value)
                for model_id, model_info in self._model_info_map.items()
            ),
            stateMap=dict(
                (model_id, model_info.state.value)
                for model_id, model_info in self._model_info_map.items()
            ),
        )

    def register_built_in_model(self, model_info: ModelInfo):
        with self._lock_pool.get_lock(model_info.model_id).write_lock():
            self._model_info_map[model_info.model_id] = model_info

    def update_model_state(self, model_id: str, state: ModelStates):
        with self._lock_pool.get_lock(model_id).write_lock():
            if model_id in self._model_info_map:
                self._model_info_map[model_id].state = state
            else:
                raise ValueError(f"Model {model_id} does not exist.")

    def get_built_in_model_type(self, model_id: str) -> BuiltInModelType:
        """
        Get the type of the model with the given model_id.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The type of the model.
        """
        with self._lock_pool.get_lock(model_id).read_lock():
            if model_id in self._model_info_map:
                return get_built_in_model_type(
                    self._model_info_map[model_id].model_type
                )
            else:
                raise ValueError(f"Model {model_id} does not exist.")
