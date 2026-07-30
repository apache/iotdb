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

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.dispatcher.abstract_dispatcher import (
    AbstractDispatcher,
)
from iotdb.ainode.core.inference.inference_request import InferenceRequest
from iotdb.ainode.core.inference.inference_request_pool import PoolState
from iotdb.ainode.core.log import Logger

logger = Logger()


class BasicDispatcher(AbstractDispatcher):
    """
    Basic dispatcher for inference requests.
    """

    def __init__(self, pool_states):
        super().__init__(pool_states)

    def _select_pool_by_hash(self, req, pool_ids) -> int:
        """
        Select a pool for the given request using a hash-based approach.
        """
        model_id = req.model_id
        if not pool_ids:
            raise InferenceModelInternalException(
                f"No available pools for model {model_id}"
            )
        start_idx = hash(req.req_id) % len(pool_ids)
        n = len(pool_ids)
        for i in range(n):
            pool_id = pool_ids[(start_idx + i) % n]
            state = self.pool_states[pool_id]
            if state == PoolState.RUNNING:
                return pool_id
        raise InferenceModelInternalException(
            f"No RUNNING pools available for model {model_id}"
        )

    def dispatch_request(self, req: InferenceRequest, pool_ids: list[int]) -> int:
        pool_idx = self._select_pool_by_hash(req, pool_ids)
        return pool_idx
