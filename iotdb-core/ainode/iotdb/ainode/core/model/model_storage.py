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
import time
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
    USER_DEFINED_MANIFEST,
    ModelCategory,
    ModelStates,
    UriType,
)
from iotdb.ainode.core.model.model_info import (
    BUILTIN_HF_TRANSFORMERS_MODEL_MAP,
    BUILTIN_SKTIME_MODEL_MAP,
    ModelInfo,
)
from iotdb.ainode.core.model.model_manifest import ModelManifest
from iotdb.ainode.core.model.utils import (
    _fetch_model_from_hf_repo,
    _fetch_model_from_local,
    ensure_init_file,
    get_parsed_uri,
    has_base_weights,
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
        # Durable registration list for user-defined models. Lives inside the
        # user_defined/ directory (beside the per-model dirs), so it is the
        # single source of truth for which user-defined models were registered.
        self._manifest = ModelManifest(
            os.path.join(self._models_dir, ModelCategory.USER_DEFINED.value)
        )
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
        # User-defined discovery is manifest-driven and self-healing, but wrap
        # it defensively so that no unexpected file-system state can ever crash
        # AINode startup (constraint: an unrecognized directory must never make
        # a restart fail). The node simply comes up with no user models in the
        # pathological case, which an operator can then investigate.
        try:
            self._discover_category(ModelCategory.USER_DEFINED)
        except Exception as e:
            logger.error(
                f"User-defined model discovery failed; continuing with none: {e}"
            )
        self._discover_category(ModelCategory.FINE_TUNED)

    def _discover_category(self, category: ModelCategory):
        """Discover all models in a category directory"""
        category_path = os.path.join(self._models_dir, category.value)
        if category == ModelCategory.BUILTIN:
            self._discover_builtin_models(category_path)
        elif category == ModelCategory.USER_DEFINED:
            self._discover_user_defined_models(category_path)
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

    def _discover_user_defined_models(self, category_path: str) -> None:
        """Discover user-defined models from the manifest (the source of truth).

        Only models listed in the manifest are registered – stray/unrecognized
        directories under ``user_defined/`` are ignored, never scanned into the
        model set, and can never crash startup. Discovery also self-heals: a
        manifest entry whose directory has vanished is pruned so the warning
        does not re-spam on every restart.
        """
        models, ok = self._manifest.load()

        if not self._manifest.file_exists():
            # First-upgrade bootstrap: an existing deployment has user_defined
            # models on disk but no manifest yet. Adopt them once so nothing
            # legitimately registered disappears on the first restart.
            models = self._bootstrap_manifest_from_disk(category_path)
        elif not ok:
            # Manifest present but unreadable (bad JSON / unknown version). Treat
            # it as an incident: quarantine the bad file for forensics, then
            # rebuild from disk best-effort rather than silently losing models.
            self._quarantine_corrupt_manifest()
            models = self._bootstrap_manifest_from_disk(category_path)

        survivors: Dict[str, dict] = {}
        for model_id, entry in models.items():
            model_dir = os.path.join(category_path, model_id)
            if not self._user_defined_payload_present(model_dir):
                logger.warning(
                    f"User-defined model '{model_id}' is listed in the manifest "
                    f"but its files are missing under {model_dir}; pruning it."
                )
                continue
            try:
                model_info = ModelInfo(
                    model_id=model_id,
                    model_type=entry.get("model_type", ""),
                    category=ModelCategory.USER_DEFINED,
                    state=ModelStates.ACTIVE,
                    pipeline_cls=entry.get("pipeline_cls", ""),
                    auto_map=entry.get("auto_map"),
                    hub_mixin_cls=entry.get("hub_mixin_cls", ""),
                    # Normalize "" (legacy/bootstrap entries) to None so a plain
                    # user-defined model is rebuilt exactly as it was registered
                    # in memory; the pipeline loader keys off `is not None`.
                    base_model_id=entry.get("base_model_id") or None,
                )
                with self._lock_pool.get_lock(model_id).write_lock():
                    self._models[ModelCategory.USER_DEFINED.value][
                        model_id
                    ] = model_info
                survivors[model_id] = entry
            except Exception as e:
                # A single malformed entry must not abort discovery of the rest.
                logger.error(f"Failed to register user-defined model '{model_id}': {e}")

        # Persist the reconciliation result once, only if it changed, to prune
        # vanished directories and rejected entries from the manifest.
        if survivors != models:
            self._manifest.replace_all(survivors)

    def _bootstrap_manifest_from_disk(self, category_path: str) -> Dict[str, dict]:
        """Build a manifest dict by scanning ``user_defined/`` directories once.

        Used for the first-upgrade bootstrap and for recovery after a corrupt
        manifest. Skips anything that is not a plausible model directory (files,
        ``__pycache__``, ``.ipynb_checkpoints``, empty/half-copied dirs), so
        strays never get adopted. Tolerates a corrupt ``config.json`` when
        weights are present (adopts with empty metadata) to preserve the old
        discovery's leniency – a real model must not vanish on upgrade.
        """
        models: Dict[str, dict] = {}
        if not os.path.isdir(category_path):
            return models
        for name in os.listdir(category_path):
            model_dir = os.path.join(category_path, name)
            if not self._user_defined_payload_present(model_dir):
                logger.info(
                    f"Skipping non-model entry '{name}' under {category_path} "
                    f"while bootstrapping the user-defined model manifest."
                )
                continue
            entry = {
                "model_type": "",
                "pipeline_cls": "",
                "auto_map": None,
                "hub_mixin_cls": "",
                "base_model_id": "",
            }
            config_path = os.path.join(model_dir, CONFIG_JSON)
            if os.path.exists(config_path):
                try:
                    config = load_model_config_in_json(config_path)
                    entry["model_type"] = config.get("model_type", "")
                    entry["auto_map"] = config.get("auto_map", None)
                    entry["pipeline_cls"] = config.get("pipeline_cls", "")
                    entry["hub_mixin_cls"] = config.get("hub_mixin_cls", "")
                    entry["base_model_id"] = config.get("base_model_id", "")
                except Exception as e:
                    logger.warning(
                        f"config.json for user-defined model '{name}' is "
                        f"unreadable ({e}); adopting it with empty metadata."
                    )
            models[name] = entry
        self._manifest.replace_all(models)
        return models

    def _user_defined_payload_present(self, model_dir: str) -> bool:
        """Whether ``model_dir`` looks like a real user-defined model directory.

        Liberal and side-effect-free (unlike ``validate_model_files``, which
        requires ``model.safetensors`` specifically and writes an ``__init__.py``).
        A directory is a model if it contains a ``config.json`` OR any recognized
        weight file – matching the old discovery's tolerance.
        """
        if not os.path.isdir(model_dir):
            return False
        return os.path.exists(os.path.join(model_dir, CONFIG_JSON)) or has_base_weights(
            model_dir
        )

    def _quarantine_corrupt_manifest(self) -> None:
        """Rename a corrupt manifest aside so it is not re-read on next restart."""
        manifest_path = os.path.join(
            self._models_dir,
            ModelCategory.USER_DEFINED.value,
            USER_DEFINED_MANIFEST,
        )
        if not os.path.exists(manifest_path):
            return
        quarantined = f"{manifest_path}.corrupt.{int(time.time())}"
        try:
            os.replace(manifest_path, quarantined)
            logger.error(
                f"Corrupt user-defined model manifest quarantined to {quarantined}; "
                f"rebuilding from disk."
            )
        except OSError as e:
            logger.error(f"Failed to quarantine corrupt manifest {manifest_path}: {e}")

    def _process_fine_tuned_model_directory(self, model_dir: str, model_id: str):
        """Handling the discovery logic for a fine-tuned model directory."""
        config_path = os.path.join(model_dir, CONFIG_JSON)
        model_type = ""
        auto_map = None
        pipeline_cls = ""
        # Load-bearing: base_model_id is pre-initialized here so that a
        # config-less fine-tuned directory does not raise NameError below (the
        # user-defined path had exactly this bug before it became manifest-driven).
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

        # Finalize config.json (base-model override) BEFORE the manifest/memory
        # so that on restart the on-disk config matches what the manifest records.
        if existing_model_id:
            # Override key configs from the base model
            config["base_model_id"] = existing_model_id
            config["auto_map"] = auto_map
            save_model_config_to_json(config_path, config)

        # The manifest entry stores exactly the fields discovery needs to rebuild
        # the ModelInfo on restart, so it never has to re-read the (possibly
        # stray) directory's config.json again.
        entry = {
            "model_type": model_type,
            "pipeline_cls": pipeline_cls,
            "auto_map": auto_map,
            "hub_mixin_cls": hub_mixin_cls,
            # Keep None (not "") for the no-base case so the value rebuilt on
            # restart matches the in-memory ModelInfo below; the pipeline loader
            # discriminates base vs plain models with `base_model_id is not None`.
            "base_model_id": existing_model_id,
        }
        # Lock order invariant: per-model write lock (outer) -> manifest lock
        # (inner). ModelManifest never takes a per-model lock, so no cycle.
        with self._lock_pool.get_lock(model_id).write_lock():
            # Re-check existence under the write lock. The top-of-method
            # is_model_registered() check uses a different lock bucket
            # (get_lock("")) and is released before the download, so two
            # concurrent register_model(model_id) calls could both pass it;
            # this makes "register model_id exactly once" hold.
            if model_id in self._models[
                ModelCategory.USER_DEFINED.value
            ] or self._manifest.contains(model_id):
                raise ModelExistedException(model_id)
            # 1. Durable manifest first: a crash after this converges toward
            #    "registered" on restart (the files are present), matching the
            #    success the client is about to be told.
            self._manifest.add(model_id, entry)
            # 2. Then in-memory, built from the same fields (single source of truth).
            self._models[ModelCategory.USER_DEFINED.value][model_id] = ModelInfo(
                model_id=model_id,
                model_type=model_type,
                category=ModelCategory.USER_DEFINED,
                state=ModelStates.ACTIVE,
                pipeline_cls=pipeline_cls,
                auto_map=auto_map,
                hub_mixin_cls=hub_mixin_cls,
                base_model_id=existing_model_id,
            )
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
            # 1. De-register from the durable manifest first (user_defined only;
            #    builtin/fine_tuned are not tracked there). Idempotent, so it is
            #    safe even if a later step fails. Lock order: per-model write lock
            #    (outer) -> manifest lock (inner).
            if model_info.category == ModelCategory.USER_DEFINED:
                self._manifest.remove(model_id)
            # 2. Then delete the files.
            model_path = os.path.join(
                self._models_dir, model_info.category.value, model_id
            )
            rmtree_error = None
            if os.path.exists(model_path):
                try:
                    shutil.rmtree(model_path)
                    logger.info(f"Model directory is deleted: {model_path}")
                except Exception as e:
                    rmtree_error = e
                    logger.error(
                        f"Failed to delete model directory {model_path}: {e}; "
                        f"the model is de-registered and a leftover directory will "
                        f"be ignored on restart."
                    )
            # 3. Then drop the in-memory entry ALWAYS – the model is logically
            #    gone (the manifest, now the source of truth, no longer lists it),
            #    so we must not leave a zombie serving a half-deleted directory.
            del self._models[category_value][model_id]
            logger.info(f"Model {model_id} has been removed from model storage")
            if rmtree_error is not None:
                # Surface the failure to the client, but only after state is
                # consistent (manifest + memory already cleared).
                raise rmtree_error

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
