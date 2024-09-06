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
from typing import Callable

from yaml import YAMLError

from iotdb.ainode.constant import TSStatusCode, BuiltInModelType
from iotdb.ainode.exception import InvalidUriError, BadConfigValueError, BuiltInModelNotSupportError
from iotdb.ainode.log import Logger
from iotdb.ainode.model.built_in_model_factory import fetch_built_in_model
from iotdb.ainode.model.model_storage import ModelStorage
from iotdb.ainode.util.status import get_status
from iotdb.thrift.ainode.ttypes import TRegisterModelReq, TRegisterModelResp, TDeleteModelReq
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()


class ModelManager:
    def __init__(self):
        self.model_storage = ModelStorage()

    def register_model(self, req: TRegisterModelReq) -> TRegisterModelResp:
        logger.info(f"register model {req.modelId} from {req.uri}")
        try:
            configs, attributes = self.model_storage.register_model(req.modelId, req.uri)
            return TRegisterModelResp(get_status(TSStatusCode.SUCCESS_STATUS), configs, attributes)
        except InvalidUriError as e:
            logger.warning(e)
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_URI_ERROR, e.message))
        except BadConfigValueError as e:
            logger.warning(e)
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, e.message))
        except YAMLError as e:
            logger.warning(e)
            self.model_storage.delete_model(req.modelId)
            if hasattr(e, 'problem_mark'):
                mark = e.problem_mark
                return TRegisterModelResp(get_status(TSStatusCode.INVALID_INFERENCE_CONFIG,
                                                     f"An error occurred while parsing the yaml file, "
                                                     f"at line {mark.line + 1} column {mark.column + 1}."))
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_INFERENCE_CONFIG, f"An error occurred while parsing the yaml file"))
        except Exception as e:
            logger.warning(e)
            self.model_storage.delete_model(req.modelId)
            return TRegisterModelResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR))

    def delete_model(self, req: TDeleteModelReq) -> TSStatus:
        logger.info(f"delete model {req.modelId}")
        try:
            self.model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def load_model(self, model_id: str, acceleration: bool = False) -> Callable:
        logger.info(f"load model {model_id}")
        return self.model_storage.load_model(model_id, acceleration)

    @staticmethod
    def load_built_in_model(model_id: str, attributes: {}):
        model_id = model_id.lower()
        if model_id not in BuiltInModelType.values():
            raise BuiltInModelNotSupportError(model_id)
        return fetch_built_in_model(model_id, attributes)
