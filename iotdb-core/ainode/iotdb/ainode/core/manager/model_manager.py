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

from typing import Any, List, Optional

from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import BuiltInModelDeletionError
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.model.model_loader import load_model
from iotdb.ainode.core.model.model_storage import ModelCategory, ModelInfo, ModelStorage
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.decorator import singleton
from iotdb.thrift.ainode.ttypes import (
    TDeleteModelReq,
    TRegisterModelReq,
    TRegisterModelResp,
    TShowModelsReq,
    TShowModelsResp,
)
from iotdb.thrift.common.ttypes import TSStatus

logger = Logger()


@singleton
class ModelManager:
    def __init__(self):
        self._model_storage = ModelStorage()

    def register_model(
        self,
        req: TRegisterModelReq,
    ) -> TRegisterModelResp:
        try:
            if self._model_storage.register_model(model_id=req.modelId, uri=req.uri):
                return TRegisterModelResp(get_status(TSStatusCode.SUCCESS_STATUS))
            return TRegisterModelResp(get_status(TSStatusCode.AINODE_INTERNAL_ERROR))
        except ValueError as e:
            return TRegisterModelResp(
                get_status(TSStatusCode.INVALID_URI_ERROR, str(e))
            )
        except Exception as e:
            return TRegisterModelResp(
                get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            )

    def show_models(self, req: TShowModelsReq) -> TShowModelsResp:
        self._refresh()
        return self._model_storage.show_models(req)

    def delete_model(self, req: TDeleteModelReq) -> TSStatus:
        try:
            self._model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except BuiltInModelDeletionError as e:
            logger.warning(e)
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
        except Exception as e:
            logger.warning(e)
            return get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))

    def get_model_info(
        self,
        model_id: str,
        category: Optional[ModelCategory] = None,
    ) -> Optional[ModelInfo]:
        return self._model_storage.get_model_info(model_id, category)

    def get_model_infos(
        self,
        category: Optional[ModelCategory] = None,
        model_type: Optional[str] = None,
    ) -> List[ModelInfo]:
        return self._model_storage.get_model_infos(category, model_type)

    def _refresh(self):
        """Refresh the model list (re-scan the file system)"""
        self._model_storage.discover_all_models()

    def get_registered_models(self) -> List[str]:
        return self._model_storage.get_registered_models()

    def is_model_registered(self, model_id: str) -> bool:
        return self._model_storage.is_model_registered(model_id)
