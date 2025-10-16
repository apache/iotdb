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
1from iotdb.ainode.core.exception import InferenceModelInternalError
1from iotdb.ainode.core.inference.dispatcher.abstract_dispatcher import (
1    AbstractDispatcher,
1)
1from iotdb.ainode.core.inference.inference_request import InferenceRequest
1from iotdb.ainode.core.inference.inference_request_pool import PoolState
1from iotdb.ainode.core.log import Logger
1
1logger = Logger()
1
1
1class BasicDispatcher(AbstractDispatcher):
1    """
1    Basic dispatcher for inference requests.
1    """
1
1    def __init__(self, pool_states):
1        super().__init__(pool_states)
1
1    def _select_pool_by_hash(self, req, pool_ids) -> int:
1        """
1        Select a pool for the given request using a hash-based approach.
1        """
1        model_id = req.model_id
1        if not pool_ids:
1            raise InferenceModelInternalError(
1                f"No available pools for model {model_id}"
1            )
1        start_idx = hash(req.req_id) % len(pool_ids)
1        n = len(pool_ids)
1        for i in range(n):
1            pool_id = pool_ids[(start_idx + i) % n]
1            state = self.pool_states[pool_id]
1            if state == PoolState.RUNNING:
1                return pool_id
1        raise InferenceModelInternalError(
1            f"No RUNNING pools available for model {model_id}"
1        )
1
1    def dispatch_request(self, req: InferenceRequest, pool_ids: list[int]) -> int:
1        pool_idx = self._select_pool_by_hash(req, pool_ids)
1        return pool_idx
1