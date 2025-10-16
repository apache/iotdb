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
1from typing import Callable, Dict
1
1from torch import nn
1from yaml import YAMLError
1
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.exception import BadConfigValueError, InvalidUriError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.model_enums import BuiltInModelType, ModelStates
1from iotdb.ainode.core.model.model_info import ModelInfo
1from iotdb.ainode.core.model.model_storage import ModelStorage
1from iotdb.ainode.core.rpc.status import get_status
1from iotdb.ainode.core.util.decorator import singleton
1from iotdb.thrift.ainode.ttypes import (
1    TDeleteModelReq,
1    TRegisterModelReq,
1    TRegisterModelResp,
1    TShowModelsReq,
1    TShowModelsResp,
1)
1from iotdb.thrift.common.ttypes import TSStatus
1
1logger = Logger()
1
1
1@singleton
1class ModelManager:
1    def __init__(self):
1        self.model_storage = ModelStorage()
1
1    def register_model(self, req: TRegisterModelReq) -> TRegisterModelResp:
1        logger.info(f"register model {req.modelId} from {req.uri}")
1        try:
1            configs, attributes = self.model_storage.register_model(
1                req.modelId, req.uri
1            )
1            return TRegisterModelResp(
1                get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes
1            )
1        except InvalidUriError as e:
1            logger.warning(e)
1            return TRegisterModelResp(
1                get_status(TSStatusCode.INVALID_URI_ERROR, e.message)
1            )
1        except BadConfigValueError as e:
1            logger.warning(e)
1            return TRegisterModelResp(
1                get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, e.message)
1            )
1        except YAMLError as e:
1            logger.warning(e)
1            if hasattr(e, "problem_mark"):
1                mark = e.problem_mark
1                return TRegisterModelResp(
1                    get_status(
1                        TSStatusCode.INVALID_INFERENCE_CONFIG,
1                        f"An error occurred while parsing the yaml file, "
1                        f"at line {mark.line + 1} column {mark.column + 1}.",
1                    )
1                )
1            return TRegisterModelResp(
1                get_status(
1                    TSStatusCode.INVALID_INFERENCE_CONFIG,
1                    f"An error occurred while parsing the yaml file",
1                )
1            )
1        except Exception as e:
1            logger.warning(e)
1            return TRegisterModelResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR))
1
1    def delete_model(self, req: TDeleteModelReq) -> TSStatus:
1        logger.info(f"delete model {req.modelId}")
1        try:
1            self.model_storage.delete_model(req.modelId)
1            return get_status(TSStatusCode.SUCCESS_STATUS)
1        except Exception as e:
1            logger.warning(e)
1            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
1
1    def load_model(
1        self, model_id: str, inference_attrs: Dict[str, str], acceleration: bool = False
1    ) -> Callable:
1        """
1        Load the model with the given model_id.
1        """
1        logger.info(f"Load model {model_id}")
1        try:
1            model = self.model_storage.load_model(
1                model_id, inference_attrs, acceleration
1            )
1            logger.info(f"Model {model_id} loaded")
1            return model
1        except Exception as e:
1            logger.error(f"Failed to load model {model_id}: {e}")
1            raise
1
1    def save_model(self, model_id: str, model: nn.Module) -> TSStatus:
1        """
1        Save the model using save_pretrained
1        """
1        logger.info(f"Saving model {model_id}")
1        try:
1            self.model_storage.save_model(model_id, model)
1            logger.info(f"Saving model {model_id} successfully")
1            return get_status(
1                TSStatusCode.SUCCESS_STATUS, f"Model {model_id} saved successfully"
1            )
1        except Exception as e:
1            logger.error(f"Save model failed: {e}")
1            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
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
1        return self.model_storage.get_ckpt_path(model_id)
1
1    def show_models(self, req: TShowModelsReq) -> TShowModelsResp:
1        return self.model_storage.show_models(req)
1
1    def register_built_in_model(self, model_info: ModelInfo):
1        self.model_storage.register_built_in_model(model_info)
1
1    def update_model_state(self, model_id: str, state: ModelStates):
1        self.model_storage.update_model_state(model_id, state)
1
1    def get_built_in_model_type(self, model_id: str) -> BuiltInModelType:
1        """
1        Get the type of the model with the given model_id.
1        """
1        return self.model_storage.get_built_in_model_type(model_id)
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
1        return self.model_storage.is_built_in_or_fine_tuned(model_id)
1