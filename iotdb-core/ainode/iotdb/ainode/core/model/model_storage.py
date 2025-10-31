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

import torch
from torch import nn

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import (
    MODEL_CONFIG_FILE_IN_JSON,
    MODEL_WEIGHTS_FILE_IN_PT,
    TSStatusCode,
)
from iotdb.ainode.core.exception import (
    BuiltInModelDeletionError,
    ModelNotExistError,
    UnsupportedError,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.built_in_model_factory import (
    download_built_in_ltsm_from_hf_if_necessary,
    fetch_built_in_model,
)
from iotdb.ainode.core.model.model_enums import (
    BuiltInModelType,
    ModelCategory,
    ModelFileType,
    ModelStates,
)
from iotdb.ainode.core.model.model_factory import fetch_model_by_uri
from iotdb.ainode.core.model.model_info import (
    BUILT_IN_LTSM_MAP,
    BUILT_IN_MACHINE_LEARNING_MODEL_MAP,
    ModelInfo,
    get_built_in_model_type,
    get_model_file_type,
)
from iotdb.ainode.core.model.uri_utils import get_model_register_strategy
from iotdb.ainode.core.util.lock import ModelLockPool
from iotdb.thrift.ainode.ttypes import TShowModelsReq, TShowModelsResp
from iotdb.thrift.common.ttypes import TSStatus

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
        # 4. retrieve user-defined models from the model directory
        user_defined_models = self._retrieve_user_defined_models()
        for model_id in user_defined_models:
            self._model_info_map[model_id] = user_defined_models[model_id]

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
            return download_built_in_ltsm_from_hf_if_necessary(
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

    def _retrieve_user_defined_models(self):
        """
        Retrieve user_defined models from the model directory.

        Returns:
            {"model_id": ModelInfo}
        """
        result = {}
        user_dirs = [
            d
            for d in os.listdir(self._model_dir)
            if os.path.isdir(os.path.join(self._model_dir, d)) and d != "weights"
        ]
        for model_id in user_dirs:
            result[model_id] = ModelInfo(
                model_id=model_id,
                model_type="",
                category=ModelCategory.USER_DEFINED,
                state=ModelStates.ACTIVE,
            )
        return result

    def register_model(self, model_id: str, uri: str):
        """
        Args:
            model_id: id of model to register
            uri: network or local dir path of the model to register
        Returns:
            configs: TConfigs
            attributes: str
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            storage_path = os.path.join(self._model_dir, f"{model_id}")
            # create storage dir if not exist
            if not os.path.exists(storage_path):
                os.makedirs(storage_path)
            uri_type, parsed_uri, model_file_type = get_model_register_strategy(uri)
            self._model_info_map[model_id] = ModelInfo(
                model_id=model_id,
                model_type="",
                category=ModelCategory.USER_DEFINED,
                state=ModelStates.LOADING,
            )
            try:
                # TODO: The uri should be fetched asynchronously
                configs, attributes = fetch_model_by_uri(
                    uri_type, parsed_uri, storage_path, model_file_type
                )
                self._model_info_map[model_id].state = ModelStates.ACTIVE
                return configs, attributes
            except Exception as e:
                logger.error(f"Failed to register model {model_id}: {e}")
                self._model_info_map[model_id].state = ModelStates.INACTIVE
                raise e

    def delete_model(self, model_id: str) -> None:
        """
        Args:
            model_id: id of model to delete
        Returns:
            None
        """
        # check if the model is built-in
        with self._lock_pool.get_lock(model_id).read_lock():
            if self._is_built_in(model_id):
                raise BuiltInModelDeletionError(model_id)

        # delete the user-defined or fine-tuned model
        with self._lock_pool.get_lock(model_id).write_lock():
            storage_path = os.path.join(self._model_dir, f"{model_id}")
            if os.path.exists(storage_path):
                shutil.rmtree(storage_path)
            storage_path = os.path.join(self._builtin_model_dir, f"{model_id}")
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
        return (
            model_id in self._model_info_map
            and self._model_info_map[model_id].category == ModelCategory.BUILT_IN
        )

    def is_built_in_or_fine_tuned(self, model_id: str) -> bool:
        """
        Check if the model_id corresponds to a built-in or fine-tuned model.

        Args:
            model_id (str): The ID of the model.

        Returns:
            bool: True if the model is built-in or fine_tuned, False otherwise.
        """
        return model_id in self._model_info_map and (
            self._model_info_map[model_id].category == ModelCategory.BUILT_IN
            or self._model_info_map[model_id].category == ModelCategory.FINE_TUNED
        )

    def load_model(
        self, model_id: str, inference_attrs: Dict[str, str], acceleration: bool
    ) -> Callable:
        """
        Load a model with automatic detection of .safetensors or .pt format

        Returns:
            model: The model instance corresponding to specific model_id
        """
        with self._lock_pool.get_lock(model_id).read_lock():
            if self.is_built_in_or_fine_tuned(model_id):
                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
                return fetch_built_in_model(
                    get_built_in_model_type(self._model_info_map[model_id].model_type),
                    model_dir,
                    inference_attrs,
                )
            else:
                # load the user-defined model
                model_dir = os.path.join(self._model_dir, f"{model_id}")
                model_file_type = get_model_file_type(model_dir)
                if model_file_type == ModelFileType.SAFETENSORS:
                    # TODO: Support this function
                    raise UnsupportedError("SAFETENSORS format")
                else:
                    model_path = os.path.join(model_dir, MODEL_WEIGHTS_FILE_IN_PT)

                    if not os.path.exists(model_path):
                        raise ModelNotExistError(model_path)
                    model = torch.jit.load(model_path)
                    if (
                        isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
                        or not acceleration
                    ):
                        return model

                    try:
                        model = torch.compile(model)
                    except Exception as e:
                        logger.warning(
                            f"acceleration failed, fallback to normal mode: {str(e)}"
                        )
                    return model

    def save_model(self, model_id: str, model: nn.Module):
        """
        Save the model using save_pretrained

        Returns:
            Whether saving succeeded
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            if self.is_built_in_or_fine_tuned(model_id):
                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
                model.save_pretrained(model_dir)
            else:
                # save the user-defined model
                model_dir = os.path.join(self._model_dir, f"{model_id}")
                os.makedirs(model_dir, exist_ok=True)
                model_path = os.path.join(model_dir, MODEL_WEIGHTS_FILE_IN_PT)
                try:
                    scripted_model = (
                        model
                        if isinstance(model, torch.jit.ScriptModule)
                        else torch.jit.script(model)
                    )
                    torch.jit.save(scripted_model, model_path)
                except Exception as e:
                    logger.error(f"Failed to save scripted model: {e}")

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

    def show_models(self, req: TShowModelsReq) -> TShowModelsResp:
        resp_status = TSStatus(
            code=TSStatusCode.SUCCESS_STATUS.value,
            message="Show models successfully",
        )
        if req.modelId:
            if req.modelId in self._model_info_map:
                model_info = self._model_info_map[req.modelId]
                return TShowModelsResp(
                    status=resp_status,
                    modelIdList=[req.modelId],
                    modelTypeMap={req.modelId: model_info.model_type},
                    categoryMap={req.modelId: model_info.category.value},
                    stateMap={req.modelId: model_info.state.value},
                )
            else:
                return TShowModelsResp(
                    status=resp_status,
                    modelIdList=[],
                    modelTypeMap={},
                    categoryMap={},
                    stateMap={},
                )
        return TShowModelsResp(
            status=resp_status,
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

    def get_model_info(self, model_id: str) -> ModelInfo:
        with self._lock_pool.get_lock(model_id).read_lock():
            if model_id in self._model_info_map:
                return self._model_info_map[model_id]
            else:
                raise ValueError(f"Model {model_id} does not exist.")

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
