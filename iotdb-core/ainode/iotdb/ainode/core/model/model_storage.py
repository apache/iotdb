1# Licensed to the Apache Software Foundation (ASF) under one
1# or more contributor license agreements.  See the NOTICE file
1# distributed with this work for additional information
1# regarding copyright ownership.  The ASF licenses this file
1# to you under the Apache License, Version 2.0 (the
1# "License"); you may not use this file except in compliance
1# with the License.  You may obtain a copy of the License at
1#
1#     http://www.apache.org/licenses/LICENSE-2.0
1#
1# Unless required by applicable law or agreed to in writing,
1# software distributed under the License is distributed on an
1# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1# KIND, either express or implied.  See the License for the
1# specific language governing permissions and limitations
1# under the License.
1#
1
1import concurrent.futures
1import json
1import os
1import shutil
1from collections.abc import Callable
1from typing import Dict
1
1import torch
1from torch import nn
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import (
1    MODEL_CONFIG_FILE_IN_JSON,
1    MODEL_WEIGHTS_FILE_IN_PT,
1    TSStatusCode,
1)
1from iotdb.ainode.core.exception import (
1    BuiltInModelDeletionError,
1    ModelNotExistError,
1    UnsupportedError,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.built_in_model_factory import (
1    download_built_in_ltsm_from_hf_if_necessary,
1    fetch_built_in_model,
1)
1from iotdb.ainode.core.model.model_enums import (
1    BuiltInModelType,
1    ModelCategory,
1    ModelFileType,
1    ModelStates,
1)
1from iotdb.ainode.core.model.model_factory import fetch_model_by_uri
1from iotdb.ainode.core.model.model_info import (
1    BUILT_IN_LTSM_MAP,
1    BUILT_IN_MACHINE_LEARNING_MODEL_MAP,
1    ModelInfo,
1    get_built_in_model_type,
1    get_model_file_type,
1)
1from iotdb.ainode.core.model.uri_utils import get_model_register_strategy
1from iotdb.ainode.core.util.lock import ModelLockPool
1from iotdb.thrift.ainode.ttypes import TShowModelsReq, TShowModelsResp
1from iotdb.thrift.common.ttypes import TSStatus
1
1logger = Logger()
1
1
1class ModelStorage(object):
1    def __init__(self):
1        self._model_dir = os.path.join(
1            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
1        )
1        if not os.path.exists(self._model_dir):
1            try:
1                os.makedirs(self._model_dir)
1            except PermissionError as e:
1                logger.error(e)
1                raise e
1        self._builtin_model_dir = os.path.join(
1            os.getcwd(), AINodeDescriptor().get_config().get_ain_builtin_models_dir()
1        )
1        if not os.path.exists(self._builtin_model_dir):
1            try:
1                os.makedirs(self._builtin_model_dir)
1            except PermissionError as e:
1                logger.error(e)
1                raise e
1        self._lock_pool = ModelLockPool()
1        self._executor = concurrent.futures.ThreadPoolExecutor(
1            max_workers=1
1        )  # TODO: Here we set the work_num=1 cause we found that the hf download interface is not stable for concurrent downloading.
1        self._model_info_map: Dict[str, ModelInfo] = {}
1        self._init_model_info_map()
1
1    def _init_model_info_map(self):
1        """
1        Initialize the model info map.
1        """
1        # 1. initialize built-in and ready-to-use models
1        for model_id in BUILT_IN_MACHINE_LEARNING_MODEL_MAP:
1            self._model_info_map[model_id] = BUILT_IN_MACHINE_LEARNING_MODEL_MAP[
1                model_id
1            ]
1        # 2. retrieve fine-tuned models from the built-in model directory
1        fine_tuned_models = self._retrieve_fine_tuned_models()
1        for model_id in fine_tuned_models:
1            self._model_info_map[model_id] = fine_tuned_models[model_id]
1        # 3. automatically downloading the weights of built-in LSTM models when necessary
1        for model_id in BUILT_IN_LTSM_MAP:
1            if model_id not in self._model_info_map:
1                self._model_info_map[model_id] = BUILT_IN_LTSM_MAP[model_id]
1            future = self._executor.submit(
1                self._download_built_in_model_if_necessary, model_id
1            )
1            future.add_done_callback(
1                lambda f, mid=model_id: self._callback_model_download_result(f, mid)
1            )
1        # 4. retrieve user-defined models from the model directory
1        user_defined_models = self._retrieve_user_defined_models()
1        for model_id in user_defined_models:
1            self._model_info_map[model_id] = user_defined_models[model_id]
1
1    def _retrieve_fine_tuned_models(self):
1        """
1        Retrieve fine-tuned models from the built-in model directory.
1
1        Returns:
1            {"model_id": ModelInfo}
1        """
1        result = {}
1        build_in_dirs = [
1            d
1            for d in os.listdir(self._builtin_model_dir)
1            if os.path.isdir(os.path.join(self._builtin_model_dir, d))
1        ]
1        for model_id in build_in_dirs:
1            config_file_path = os.path.join(
1                self._builtin_model_dir, model_id, MODEL_CONFIG_FILE_IN_JSON
1            )
1            if os.path.isfile(config_file_path):
1                with open(config_file_path, "r") as f:
1                    model_config = json.load(f)
1                if "model_type" in model_config:
1                    model_type = model_config["model_type"]
1                    model_info = ModelInfo(
1                        model_id=model_id,
1                        model_type=model_type,
1                        category=ModelCategory.FINE_TUNED,
1                        state=ModelStates.ACTIVE,
1                    )
1                    # Refactor the built-in model category
1                    if "timer_xl" == model_id:
1                        model_info.category = ModelCategory.BUILT_IN
1                    if "sundial" == model_id:
1                        model_info.category = ModelCategory.BUILT_IN
1                    # Compatible patch with the codes in HuggingFace
1                    if "timer" == model_type:
1                        model_info.model_type = BuiltInModelType.TIMER_XL.value
1                    if "sundial" == model_type:
1                        model_info.model_type = BuiltInModelType.SUNDIAL.value
1                    result[model_id] = model_info
1        return result
1
1    def _download_built_in_model_if_necessary(self, model_id: str) -> bool:
1        """
1        Download the built-in model if it is not already downloaded.
1
1        Args:
1            model_id (str): The ID of the model to download.
1
1        Return:
1            bool: True if the model is existed or downloaded successfully, False otherwise.
1        """
1        with self._lock_pool.get_lock(model_id).write_lock():
1            local_dir = os.path.join(self._builtin_model_dir, model_id)
1            return download_built_in_ltsm_from_hf_if_necessary(
1                get_built_in_model_type(self._model_info_map[model_id].model_type),
1                local_dir,
1            )
1
1    def _callback_model_download_result(self, future, model_id: str):
1        with self._lock_pool.get_lock(model_id).write_lock():
1            if future.result():
1                self._model_info_map[model_id].state = ModelStates.ACTIVE
1                logger.info(
1                    f"The built-in model: {model_id} is active and ready to use."
1                )
1            else:
1                self._model_info_map[model_id].state = ModelStates.INACTIVE
1
1    def _retrieve_user_defined_models(self):
1        """
1        Retrieve user_defined models from the model directory.
1
1        Returns:
1            {"model_id": ModelInfo}
1        """
1        result = {}
1        user_dirs = [
1            d
1            for d in os.listdir(self._model_dir)
1            if os.path.isdir(os.path.join(self._model_dir, d)) and d != "weights"
1        ]
1        for model_id in user_dirs:
1            result[model_id] = ModelInfo(
1                model_id=model_id,
1                model_type="",
1                category=ModelCategory.USER_DEFINED,
1                state=ModelStates.ACTIVE,
1            )
1        return result
1
1    def register_model(self, model_id: str, uri: str):
1        """
1        Args:
1            model_id: id of model to register
1            uri: network or local dir path of the model to register
1        Returns:
1            configs: TConfigs
1            attributes: str
1        """
1        with self._lock_pool.get_lock(model_id).write_lock():
1            storage_path = os.path.join(self._model_dir, f"{model_id}")
1            # create storage dir if not exist
1            if not os.path.exists(storage_path):
1                os.makedirs(storage_path)
1            uri_type, parsed_uri, model_file_type = get_model_register_strategy(uri)
1            self._model_info_map[model_id] = ModelInfo(
1                model_id=model_id,
1                model_type="",
1                category=ModelCategory.USER_DEFINED,
1                state=ModelStates.LOADING,
1            )
1            try:
1                # TODO: The uri should be fetched asynchronously
1                configs, attributes = fetch_model_by_uri(
1                    uri_type, parsed_uri, storage_path, model_file_type
1                )
1                self._model_info_map[model_id].state = ModelStates.ACTIVE
1                return configs, attributes
1            except Exception as e:
1                logger.error(f"Failed to register model {model_id}: {e}")
1                self._model_info_map[model_id].state = ModelStates.INACTIVE
1                raise e
1
1    def delete_model(self, model_id: str) -> None:
1        """
1        Args:
1            model_id: id of model to delete
1        Returns:
1            None
1        """
1        # check if the model is built-in
1        with self._lock_pool.get_lock(model_id).read_lock():
1            if self._is_built_in(model_id):
1                raise BuiltInModelDeletionError(model_id)
1
1        # delete the user-defined or fine-tuned model
1        with self._lock_pool.get_lock(model_id).write_lock():
1            storage_path = os.path.join(self._model_dir, f"{model_id}")
1            if os.path.exists(storage_path):
1                shutil.rmtree(storage_path)
1            storage_path = os.path.join(self._builtin_model_dir, f"{model_id}")
1            if os.path.exists(storage_path):
1                shutil.rmtree(storage_path)
1            if model_id in self._model_info_map:
1                del self._model_info_map[model_id]
1                logger.info(f"Model {model_id} deleted successfully.")
1
1    def _is_built_in(self, model_id: str) -> bool:
1        """
1        Check if the model_id corresponds to a built-in model.
1
1        Args:
1            model_id (str): The ID of the model.
1
1        Returns:
1            bool: True if the model is built-in, False otherwise.
1        """
1        return (
1            model_id in self._model_info_map
1            and self._model_info_map[model_id].category == ModelCategory.BUILT_IN
1        )
1
1    def is_built_in_or_fine_tuned(self, model_id: str) -> bool:
1        """
1        Check if the model_id corresponds to a built-in or fine-tuned model.
1
1        Args:
1            model_id (str): The ID of the model.
1
1        Returns:
1            bool: True if the model is built-in or fine_tuned, False otherwise.
1        """
1        return model_id in self._model_info_map and (
1            self._model_info_map[model_id].category == ModelCategory.BUILT_IN
1            or self._model_info_map[model_id].category == ModelCategory.FINE_TUNED
1        )
1
1    def load_model(
1        self, model_id: str, inference_attrs: Dict[str, str], acceleration: bool
1    ) -> Callable:
1        """
1        Load a model with automatic detection of .safetensors or .pt format
1
1        Returns:
1            model: The model instance corresponding to specific model_id
1        """
1        with self._lock_pool.get_lock(model_id).read_lock():
1            if self.is_built_in_or_fine_tuned(model_id):
1                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
1                return fetch_built_in_model(
1                    get_built_in_model_type(self._model_info_map[model_id].model_type),
1                    model_dir,
1                    inference_attrs,
1                )
1            else:
1                # load the user-defined model
1                model_dir = os.path.join(self._model_dir, f"{model_id}")
1                model_file_type = get_model_file_type(model_dir)
1                if model_file_type == ModelFileType.SAFETENSORS:
1                    # TODO: Support this function
1                    raise UnsupportedError("SAFETENSORS format")
1                else:
1                    model_path = os.path.join(model_dir, MODEL_WEIGHTS_FILE_IN_PT)
1
1                    if not os.path.exists(model_path):
1                        raise ModelNotExistError(model_path)
1                    model = torch.jit.load(model_path)
1                    if (
1                        isinstance(model, torch._dynamo.eval_frame.OptimizedModule)
1                        or not acceleration
1                    ):
1                        return model
1
1                    try:
1                        model = torch.compile(model)
1                    except Exception as e:
1                        logger.warning(
1                            f"acceleration failed, fallback to normal mode: {str(e)}"
1                        )
1                    return model
1
1    def save_model(self, model_id: str, model: nn.Module):
1        """
1        Save the model using save_pretrained
1
1        Returns:
1            Whether saving succeeded
1        """
1        with self._lock_pool.get_lock(model_id).write_lock():
1            if self.is_built_in_or_fine_tuned(model_id):
1                model_dir = os.path.join(self._builtin_model_dir, f"{model_id}")
1                model.save_pretrained(model_dir)
1            else:
1                # save the user-defined model
1                model_dir = os.path.join(self._model_dir, f"{model_id}")
1                os.makedirs(model_dir, exist_ok=True)
1                model_path = os.path.join(model_dir, MODEL_WEIGHTS_FILE_IN_PT)
1                try:
1                    scripted_model = (
1                        model
1                        if isinstance(model, torch.jit.ScriptModule)
1                        else torch.jit.script(model)
1                    )
1                    torch.jit.save(scripted_model, model_path)
1                except Exception as e:
1                    logger.error(f"Failed to save scripted model: {e}")
1
1    def get_ckpt_path(self, model_id: str) -> str:
1        """
1        Get the checkpoint path for a given model ID.
1
1        Args:
1            model_id (str): The ID of the model.
1
1        Returns:
1            str: The path to the checkpoint file for the model.
1        """
1        # Only support built-in models for now
1        return os.path.join(self._builtin_model_dir, f"{model_id}")
1
1    def show_models(self, req: TShowModelsReq) -> TShowModelsResp:
1        resp_status = TSStatus(
1            code=TSStatusCode.SUCCESS_STATUS.value,
1            message="Show models successfully",
1        )
1        if req.modelId:
1            if req.modelId in self._model_info_map:
1                model_info = self._model_info_map[req.modelId]
1                return TShowModelsResp(
1                    status=resp_status,
1                    modelIdList=[req.modelId],
1                    modelTypeMap={req.modelId: model_info.model_type},
1                    categoryMap={req.modelId: model_info.category.value},
1                    stateMap={req.modelId: model_info.state.value},
1                )
1            else:
1                return TShowModelsResp(
1                    status=resp_status,
1                    modelIdList=[],
1                    modelTypeMap={},
1                    categoryMap={},
1                    stateMap={},
1                )
1        return TShowModelsResp(
1            status=resp_status,
1            modelIdList=list(self._model_info_map.keys()),
1            modelTypeMap=dict(
1                (model_id, model_info.model_type)
1                for model_id, model_info in self._model_info_map.items()
1            ),
1            categoryMap=dict(
1                (model_id, model_info.category.value)
1                for model_id, model_info in self._model_info_map.items()
1            ),
1            stateMap=dict(
1                (model_id, model_info.state.value)
1                for model_id, model_info in self._model_info_map.items()
1            ),
1        )
1
1    def register_built_in_model(self, model_info: ModelInfo):
1        with self._lock_pool.get_lock(model_info.model_id).write_lock():
1            self._model_info_map[model_info.model_id] = model_info
1
1    def update_model_state(self, model_id: str, state: ModelStates):
1        with self._lock_pool.get_lock(model_id).write_lock():
1            if model_id in self._model_info_map:
1                self._model_info_map[model_id].state = state
1            else:
1                raise ValueError(f"Model {model_id} does not exist.")
1
1    def get_built_in_model_type(self, model_id: str) -> BuiltInModelType:
1        """
1        Get the type of the model with the given model_id.
1
1        Args:
1            model_id (str): The ID of the model.
1
1        Returns:
1            str: The type of the model.
1        """
1        with self._lock_pool.get_lock(model_id).read_lock():
1            if model_id in self._model_info_map:
1                return get_built_in_model_type(
1                    self._model_info_map[model_id].model_type
1                )
1            else:
1                raise ValueError(f"Model {model_id} does not exist.")
1