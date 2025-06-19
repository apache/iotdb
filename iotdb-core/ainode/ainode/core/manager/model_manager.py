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
import threading
import time
from typing import Callable

from torch import nn
from yaml import YAMLError

from ainode.core.client import ClientManager
from ainode.core.config import AINodeDescriptor
from ainode.core.constant import STATUS_CODE_MAP, BuiltInModelType, TSStatusCode
from ainode.core.exception import (
    BadConfigValueError,
    BuiltInModelNotSupportError,
    ConfigValidationError,
    InvalidUriError,
    ModelFormatError,
    ModelLoadingError,
    WeightFileError,
)
from ainode.core.log import Logger
from ainode.core.model.built_in_model_factory import fetch_built_in_model
from ainode.core.model.model_storage import ModelStorage
from ainode.core.util.lock import ReadWriteLock
from ainode.core.util.status import get_status
from ainode.thrift.ainode.ttypes import (
    TDeleteModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
)
from ainode.thrift.common.ttypes import TSStatus

logger = Logger()


class ModelManager:
    def __init__(self):
        self.model_storage = ModelStorage()

    def register_model(self, req: TRegisterModelReq) -> TRegisterModelResp:
        logger.info(f"Starting register model {req.modelId} from {req.uri}")

        try:
            configs, attributes = self.model_storage.register_model(
                req.modelId, req.uri
            )

            # Validate the model files after registration
            try:
                validation_result = self.model_storage.validate_model_files(req.modelId)
                if not validation_result["valid"]:
                    error_msg = (
                        f"Model validation failed: {validation_result['errors']}"
                    )
                    logger.error(error_msg)
                    self.model_storage.delete_model(req.modelId)
                    return TRegisterModelResp(
                        get_status(TSStatusCode.INVALID_URI_ERROR, error_msg)
                    )

                # Log model format
                model_format = validation_result.get("format", "unknown")
                logger.info(
                    f"Model {req.modelId} registered, with format: {model_format}"
                )

            except Exception as e:
                logger.warning(f"Registered model without validation: {e}")

            return TRegisterModelResp(
                get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes
            )

        except InvalidUriError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, e.message)
            )
        except BadConfigValueError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, e.message)
            )
        except (
            ModelLoadingError,
            ModelFormatError,
            ConfigValidationError,
            WeightFileError,
        ) as e:
            logger.warning(f"IoTDB model failed: {e}")
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, str(e))
            )
        except YAMLError as e:
            logger.warning(e)
            self._update_model_status(req.modelId, "ERROR", "YAML parsing error")
            self.model_storage.delete_model(req.modelId)
            if hasattr(e, "problem_mark"):
                mark = e.problem_mark
                return TRegisterModelResp(
                    get_status(
                        TSStatusCode.INVALID_INFERENCE_CONFIG,
                        f"An error occurred while parsing the YAML file at line {mark.line + 1} column {mark.column + 1}.",
                    )
                )
            return TRegisterModelResp(
                get_status(
                    TSStatusCode.INVALID_INFERENCE_CONFIG,
                    "An error occurred while parsing the YAML file",
                )
            )
        except Exception as e:
            logger.warning(f"Unknown error: {e}")
            self._update_model_status(req.modelId, "ERROR", str(e))
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(
                get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            )

    def delete_model(self, req: TDeleteModelReq) -> TSStatus:
        logger.info(f"Delete model {req.modelId}")
        try:
            self.model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def load_model(self, model_id: str, is_built_in: bool, acceleration: bool = False) -> Callable:
        """
        Load the model with the given model_id.
        """
        logger.info(f"Load model {model_id}")
        try:
            model = self.model_storage.load_model(model_id.lower(), is_built_in, acceleration)
            logger.info(f"Model {model_id} loaded")
            return model
        except Exception as e:
            logger.error(f"Failed to load model {model_id}: {e}")
            raise

    def save_model(self, model_id: str, is_built_in: bool, model: nn.Module) -> TSStatus:
        """
        Save the model using save_pretrained
        """
        logger.info(f"Saving model {model_id}")
        try:
            self.model_storage.save_model(model_id, is_built_in, model)
            logger.info(f"Saving model {model_id} successfully")
            return get_status(
                TSStatusCode.SUCCESS_STATUS, f"Model {model_id} saved successfully"
            )
        except Exception as e:
            logger.error(f"Save model failed: {e}")
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def get_ckpt_path(self, model_id: str) -> str:
        """
        Get the checkpoint path for a given model ID.

        Args:
            model_id (str): The ID of the model.

        Returns:
            str: The path to the checkpoint file for the model.
        """
        return self.model_storage.get_ckpt_path(model_id)
