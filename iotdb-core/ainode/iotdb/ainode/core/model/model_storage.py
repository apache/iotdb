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
from typing import Dict, Optional

from huggingface_hub import hf_hub_download

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import (
    BuiltInModelDeletionException,
    ModelExistedException,
    ModelNotExistException,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_constants import (
    CONFIG_JSON,
    MODEL_SAFETENSORS,
    ModelCategory,
    ModelStates,
    UriType,
)
from iotdb.ainode.core.model.model_info import (
    BUILTIN_HF_TRANSFORMERS_MODEL_MAP,
    BUILTIN_SKTIME_MODEL_MAP,
    ModelInfo,
)
from iotdb.ainode.core.model.utils import (
    _fetch_model_from_hf_repo,
    _fetch_model_from_local,
    ensure_init_file,
    get_parsed_uri,
    load_model_config_in_json,
    parse_uri_type,
    save_model_config_to_json,
    validate_model_files,
)
from iotdb.ainode.core.util.lock import ModelLockPool
from iotdb.thrift.ainode.ttypes import TShowModelsReq, TShowModelsResp
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()


class ModelStorage:
    """Model storage class - unified management of model discovery and registration"""

    def __init__(self):
        self._models_dir = os.path.join(
            os.getcwd(), AINodeDescriptor().get_config().get_ain_models_dir()
        )
        # Unified storage: category -> {model_id -> ModelInfo}
        self._models: Dict[str, Dict[str, ModelInfo]] = {
            ModelCategory.BUILTIN.value: {},
            ModelCategory.USER_DEFINED.value: {},
            ModelCategory.FINE_TUNED.value: {},
        }
        # Async download executor
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        # Thread lock pool for protecting concurrent access to model information
        self._lock_pool = ModelLockPool()
        self._initialize_directories()
        self.discover_all_models()

    def _initialize_directories(self):
        """Initialize directory structure and ensure __init__.py files exist"""
        os.makedirs(self._models_dir, exist_ok=True)
        ensure_init_file(self._models_dir)
        for category in ModelCategory:
            category_path = os.path.join(self._models_dir, category.value)
            os.makedirs(category_path, exist_ok=True)
            ensure_init_file(category_path)

    # ==================== Discovery Methods ====================

    def discover_all_models(self):
        """Scan file system to discover all models"""
        self._discover_category(ModelCategory.BUILTIN)
        self._discover_category(ModelCategory.USER_DEFINED)
        self._discover_category(ModelCategory.FINE_TUNED)

    def _discover_category(self, category: ModelCategory):
        """Discover all models in a category directory"""
        category_path = os.path.join(self._models_dir, category.value)
        if category == ModelCategory.BUILTIN:
            self._discover_builtin_models(category_path)
        elif category == ModelCategory.USER_DEFINED:
            for model_id in os.listdir(category_path):
                if os.path.isdir(os.path.join(category_path, model_id)):
                    self._process_user_defined_model_directory(
                        os.path.join(category_path, model_id), model_id
                    )
        elif category == ModelCategory.FINE_TUNED:
            for model_id in os.listdir(category_path):
                if os.path.isdir(os.path.join(category_path, model_id)):
                    self._process_fine_tuned_model_directory(
                        os.path.join(category_path, model_id), model_id
                    )

    def _discover_builtin_models(self, category_path: str):
        # Register SKTIME models directly from map
        for model_id in BUILTIN_SKTIME_MODEL_MAP.keys():
            with self._lock_pool.get_lock(model_id).write_lock():
                self._models[ModelCategory.BUILTIN.value][model_id] = (
                    BUILTIN_SKTIME_MODEL_MAP[model_id]
                )

        # Process HuggingFace Transformers models
        for model_id in BUILTIN_HF_TRANSFORMERS_MODEL_MAP.keys():
            model_dir = os.path.join(category_path, model_id)
            os.makedirs(model_dir, exist_ok=True)
            self._process_builtin_model_directory(model_dir, model_id)

    def _process_builtin_model_directory(self, model_dir: str, model_id: str):
        """Handling the discovery logic for a builtin model directory."""
        ensure_init_file(model_dir)
        with self._lock_pool.get_lock(model_id).write_lock():
            # Check if model already exists and is in a valid state
            existing_model = self._models[ModelCategory.BUILTIN.value].get(model_id)
            if existing_model:
                # If model is already ACTIVATING or ACTIVE, skip duplicate download
                if existing_model.state in (ModelStates.ACTIVATING, ModelStates.ACTIVE):
                    return

            # If model not exists or is INACTIVE, we'll try to update its info and download its weights
            self._models[ModelCategory.BUILTIN.value][model_id] = (
                BUILTIN_HF_TRANSFORMERS_MODEL_MAP[model_id]
            )
            self._models[ModelCategory.BUILTIN.value][
                model_id
            ].state = ModelStates.ACTIVATING

        def _download_model_if_necessary() -> bool:
            """Returns: True if the model is existed or downloaded successfully, False otherwise."""
            repo_id = BUILTIN_HF_TRANSFORMERS_MODEL_MAP[model_id].repo_id
            if repo_id == "":
                return False
            weights_path = os.path.join(model_dir, MODEL_SAFETENSORS)
            config_path = os.path.join(model_dir, CONFIG_JSON)
            if not os.path.exists(weights_path):
                try:
                    hf_hub_download(
                        repo_id=repo_id,
                        filename=MODEL_SAFETENSORS,
                        local_dir=model_dir,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to download model weights from HuggingFace: {e}"
                    )
                    return False
            if not os.path.exists(config_path):
                try:
                    hf_hub_download(
                        repo_id=repo_id,
                        filename=CONFIG_JSON,
                        local_dir=model_dir,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to download model config from HuggingFace: {e}"
                    )
                    return False
            return True

        future = self._executor.submit(_download_model_if_necessary)
        future.add_done_callback(
            lambda f, mid=model_id: self._callback_model_download_result(f, mid)
        )

    def _callback_model_download_result(self, future, model_id: str):
        """Callback function for handling model download results"""
        with self._lock_pool.get_lock(model_id).write_lock():
            try:
                if future.result():
                    model_info = self._models[ModelCategory.BUILTIN.value][model_id]
                    model_info.state = ModelStates.ACTIVE
                    config_path = os.path.join(
                        self._models_dir,
                        ModelCategory.BUILTIN.value,
                        model_id,
                        CONFIG_JSON,
                    )
                    if os.path.exists(config_path):
                        with open(config_path, "r", encoding="utf-8") as f:
                            config = json.load(f)
                        model_info.model_type = config.get(
                            "model_type", model_info.model_type
                        )
                        model_info.auto_map = config.get(
                            "auto_map", model_info.auto_map
                        )
                    logger.info(
                        f"Model {model_id} downloaded successfully and is ready to use."
                    )
                else:
                    self._models[ModelCategory.BUILTIN.value][
                        model_id
                    ].state = ModelStates.INACTIVE
                    if (
                        self._models[ModelCategory.BUILTIN.value][model_id].repo_id
                        != ""
                    ):
                        logger.warning(f"Failed to download model {model_id}.")
            except Exception as e:
                logger.error(f"Error in download callback for model {model_id}: {e}")
                self._models[ModelCategory.BUILTIN.value][
                    model_id
                ].state = ModelStates.INACTIVE

    def _process_user_defined_model_directory(self, model_dir: str, model_id: str):
        """Handling the discovery logic for a user-defined model directory."""
        config_path = os.path.join(model_dir, CONFIG_JSON)
        model_type = ""
        auto_map = {}
        pipeline_cls = ""
        hub_mixin_cls = ""
        if os.path.exists(config_path):
            config = load_model_config_in_json(config_path)
            model_type = config.get("model_type", "")
            auto_map = config.get("auto_map", None)
            pipeline_cls = config.get("pipeline_cls", "")
            hub_mixin_cls = config.get("hub_mixin_cls", "")
            base_model_id = config.get("base_model_id", "")
        model_info = ModelInfo(
            model_id=model_id,
            model_type=model_type,
            category=ModelCategory.USER_DEFINED,
            state=ModelStates.ACTIVE,
            pipeline_cls=pipeline_cls,
            auto_map=auto_map,
            hub_mixin_cls=hub_mixin_cls,
            base_model_id=base_model_id,
        )
        with self._lock_pool.get_lock(model_id).write_lock():
            self._models[ModelCategory.USER_DEFINED.value][model_id] = model_info

    def _process_fine_tuned_model_directory(self, model_dir: str, model_id: str):
        """Handling the discovery logic for a fine-tuned model directory."""
        config_path = os.path.join(model_dir, CONFIG_JSON)
        model_type = ""
        auto_map = None
        pipeline_cls = ""
        base_model_id = None

        if os.path.exists(config_path):
            config = load_model_config_in_json(config_path)
            model_type = config.get("model_type", "")
            auto_map = config.get("auto_map", None)
            pipeline_cls = config.get("pipeline_cls", "")
            base_model_id = config.get("base_model_id", None)

        with self._lock_pool.get_lock(model_id).write_lock():
            model_info = ModelInfo(
                model_id=model_id,
                model_type=model_type,
                category=ModelCategory.FINE_TUNED,
                state=ModelStates.ACTIVE,
                pipeline_cls=pipeline_cls,
                auto_map=auto_map,
                base_model_id=base_model_id,
            )
            self._models[ModelCategory.FINE_TUNED.value][model_id] = model_info

    # ==================== Registration Methods ====================

    def register_model(self, model_id: str, uri: str, existing_model_id: str):
        """
        Register a user-defined model from a given URI.
        Args:
            model_id (str): Unique identifier for the model.
            uri (str): URI to fetch the model from.
            Supported URI formats:
                - file://<local_path>
                - repo://<huggingface_repo_id> (Maybe in the future)
        Raises:
            ModelExistedException: If the model_id already exists.
            InvalidModelUriException: If the URI format is invalid.
        """

        if self.is_model_registered(model_id):
            raise ModelExistedException(model_id)

        uri_type = parse_uri_type(uri)
        parsed_uri = get_parsed_uri(uri)

        model_dir = os.path.join(
            self._models_dir, ModelCategory.USER_DEFINED.value, model_id
        )
        os.makedirs(model_dir, exist_ok=True)
        ensure_init_file(model_dir)

        if uri_type == UriType.REPO:
            _fetch_model_from_hf_repo(parsed_uri, model_dir)
        else:
            _fetch_model_from_local(os.path.expanduser(parsed_uri), model_dir)

        config_path, _ = validate_model_files(model_dir)
        config = load_model_config_in_json(config_path)
        model_type = config.get("model_type", "")
        auto_map = config.get("auto_map")
        if existing_model_id is not None:
            with self._lock_pool.get_lock(existing_model_id).read_lock():
                auto_map = self._models[ModelCategory.BUILTIN.value][
                    existing_model_id
                ].auto_map
        pipeline_cls = config.get("pipeline_cls", "")
        hub_mixin_cls = config.get("hub_mixin_cls", "")

        with self._lock_pool.get_lock(model_id).write_lock():
            model_info = ModelInfo(
                model_id=model_id,
                model_type=model_type,
                category=ModelCategory.USER_DEFINED,
                state=ModelStates.ACTIVE,
                pipeline_cls=pipeline_cls,
                auto_map=auto_map,
                hub_mixin_cls=hub_mixin_cls,
                base_model_id=existing_model_id,
            )
            self._models[ModelCategory.USER_DEFINED.value][model_id] = model_info

        if existing_model_id:
            # Override key configs from the base model
            config["base_model_id"] = existing_model_id
            config["auto_map"] = auto_map
            save_model_config_to_json(config_path, config)
        logger.info(f"Successfully registered model {model_id} from URI: {uri}")

    def register_finetuned_model(self, model_id: str, base_model_id: str) -> ModelInfo:
        if self.is_model_registered(model_id):
            raise ModelExistedException(model_id)

        model_dir = os.path.join(
            self._models_dir, ModelCategory.FINE_TUNED.value, model_id
        )
        os.makedirs(model_dir, exist_ok=True)
        ensure_init_file(model_dir)

        base_model_info = self.get_model_info(base_model_id)
        if base_model_info is None:
            raise ModelNotExistException(base_model_id)

        with self._lock_pool.get_lock(model_id).write_lock():
            sft_model_info = base_model_info.copy(
                model_id=model_id,
                category=ModelCategory.FINE_TUNED,
                state=ModelStates.TRAINING,
                base_model_id=base_model_id,
            )
            self._models[ModelCategory.FINE_TUNED.value][model_id] = sft_model_info

        logger.info(f"Registered fine-tuned model {model_id} based on {base_model_id}")
        return base_model_info

    def complete_finetune(self, model_id: str) -> None:
        """Mark a fine-tuned model as ACTIVE after training completes.

        Note: ``config.json`` (including AINode metadata such as
        ``base_model_id`` and ``category``) is already written by
        ``Trainer.save_model()`` during the training worker – no
        additional file I/O is needed here.
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            model_info = self._models[ModelCategory.FINE_TUNED.value].get(model_id)
            if model_info is None:
                raise ModelNotExistException(model_id)
            model_info.state = ModelStates.ACTIVE

        logger.info(f"Fine-tuned model {model_id} is now ACTIVE")

    def fail_finetune(self, model_id: str, cleanup: bool = False) -> None:
        with self._lock_pool.get_lock(model_id).write_lock():
            model_info = self._models[ModelCategory.FINE_TUNED.value].get(model_id)
            if model_info is None:
                return

            if cleanup:
                model_path = os.path.join(
                    self._models_dir, ModelCategory.FINE_TUNED.value, model_id
                )
                if os.path.exists(model_path):
                    shutil.rmtree(model_path)
                del self._models[ModelCategory.FINE_TUNED.value][model_id]
                logger.info(f"Cleaned up failed fine-tuned model {model_id}")
            else:
                model_info.state = ModelStates.INACTIVE
                logger.warning(f"Fine-tuned model {model_id} marked as INACTIVE")
    # ==================== Show and Delete Models ====================

    def show_models(self, req: TShowModelsReq) -> TShowModelsResp:
        resp_status = TSStatus(
            code=TSStatusCode.SUCCESS_STATUS.value,
            message="Show models successfully",
        )
        if req.modelId:
            # Find specified model
            model_info = None
            for category_dict in self._models.values():
                if req.modelId in category_dict:
                    model_info = category_dict[req.modelId]
                    break

            if model_info:
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
        # Return all models
        model_id_list = []
        model_type_map = {}
        category_map = {}
        state_map = {}

        for category_dict in self._models.values():
            for model_id, model_info in category_dict.items():
                model_id_list.append(model_id)
                model_type_map[model_id] = model_info.model_type
                category_map[model_id] = model_info.category.value
                state_map[model_id] = model_info.state.value

        return TShowModelsResp(
            status=resp_status,
            modelIdList=model_id_list,
            modelTypeMap=model_type_map,
            categoryMap=category_map,
            stateMap=state_map,
        )

    def delete_model(self, model_id: str):
        """
        Delete a user-defined model by model_id.
        Args:
            model_id (str): Unique identifier for the model to be deleted.
        Raises:
            ModelNotExistException: If the model_id does not exist.
            BuiltInModelDeletionException: If attempting to delete a built-in model.
            Others: Any exceptions raised during file deletion.
        """
        with self._lock_pool.get_lock(model_id).write_lock():
            model_info = None
            category_value = None
            for cat_value, category_dict in self._models.items():
                if model_id in category_dict:
                    model_info = category_dict[model_id]
                    category_value = cat_value
                    break
            if not model_info:
                logger.warning(f"Model {model_id} does not exist, cannot delete")
                raise ModelNotExistException(model_id)
            if model_info.category == ModelCategory.BUILTIN:
                logger.warning(f"Model {model_id} is builtin, cannot delete")
                raise BuiltInModelDeletionException(model_id)
            model_info.state = ModelStates.DROPPING
            model_path = os.path.join(
                self._models_dir, model_info.category.value, model_id
            )
            if os.path.exists(model_path):
                try:
                    shutil.rmtree(model_path)
                    logger.info(f"Model directory is deleted: {model_path}")
                except Exception as e:
                    logger.error(f"Failed to delete model directory {model_path}: {e}")
                    raise e
            del self._models[category_value][model_id]
            logger.info(f"Model {model_id} has been removed from model storage")

    # ==================== Query Methods ====================

    def get_model_info(
        self, model_id: str, category: Optional[ModelCategory] = None
    ) -> Optional[ModelInfo]:
        """
        Get specified model information.
        Args:
            model_id (str): Unique identifier for the model.
            category (Optional[ModelCategory]): Category of the model (if known).
        Returns:
            ModelInfo: Information of the specified model.
        Raises:
            ModelNotExistException: If the model_id does not exist.
        """
        if category:
            # Category specified, only need to access specific dictionary, use model_id's lock
            with self._lock_pool.get_lock(model_id).read_lock():
                return self._models[category.value].get(model_id)
        else:
            # Category not specified, need to traverse all dictionaries, use global lock
            with self._lock_pool.get_lock(model_id).read_lock():
                for category_dict in self._models.values():
                    if model_id in category_dict:
                        return category_dict[model_id]
        raise ModelNotExistException(model_id)

    def get_model_info_via_model_type(
        self, model_type: str, category: Optional[ModelCategory] = None
    ) -> Optional[ModelInfo]:
        """
        Get specified model information via model_type.
        Args:
            model_type (str): The model_type defined in the model's config.json.
            category (Optional[ModelCategory]): Category of the model (if known).
        Returns:
            ModelInfo: Information of the specified model.
        Raises:
            ModelNotExistException: If the model_type does not exist.
        """
        if category:
            # Category specified, only need to access specific dictionary, use model_type's lock
            with self._lock_pool.get_lock(model_type).read_lock():
                for model_info in self._models[category.value].values():
                    if model_info.model_type == model_type:
                        return model_info
        else:
            # Category not specified, need to traverse all dictionaries, use global lock
            with self._lock_pool.get_lock("").read_lock():
                for category_dict in self._models.values():
                    for model_info in category_dict.values():
                        if model_info.model_type == model_type:
                            return model_info
        raise ModelNotExistException(model_type)

    def is_model_registered(self, model_id: str) -> bool:
        """Check if model is registered (search in _models)"""
        with self._lock_pool.get_lock("").read_lock():
            for category_dict in self._models.values():
                if model_id in category_dict:
                    return True
            return False

    def update_model_state(self, model_id: str, state: ModelStates):
        self._models[ModelCategory.FINE_TUNED.value][model_id].state = state
