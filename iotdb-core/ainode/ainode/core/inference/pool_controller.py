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
from collections import defaultdict
from typing import Dict, Optional

import torch
import torch.multiprocessing as mp

from ainode.core.exception import (
    InferenceModelInternalError,
)
from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.inference.inference_request_pool import InferenceRequestPool, PoolState
from ainode.core.inference.inference_request_pool_group import PoolGroup
from ainode.core.log import Logger

logger = Logger()


class PoolController:
    """
    A controller for handling inference request pools.
    It handles the registration of pools, adding and removing requests,
    and managing the state of each pool.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self):
        # structure: {model_id: {pool_id: PoolState}}
        self.pool_states: Dict[str, Dict[int, PoolState]] = defaultdict(dict)
        # structure: {model_id: PoolGroup}
        self._request_pool_map: Dict[str, PoolGroup] = {}

    def dispatch_request(self, model_id, req: InferenceRequest):
        pool_idx = self._select_pool_by_hash(model_id, req.req_id)
        self.add_request(pool_idx, req)
        logger.debug(
            f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_idx}][ID-{req.req_id}] Request is queued for inference"
        )

    def _select_pool_by_hash(self, model_id, req_id) -> int:
        pool_ids = self.get_pool_ids(model_id)
        if not pool_ids:
            raise InferenceModelInternalError(
                f"No available pools for model {model_id}"
            )
        start_idx = hash(req_id) % len(pool_ids)
        n = len(pool_ids)
        for i in range(n):
            pool_id = pool_ids[(start_idx + i) % n]
            state = self.get_state(model_id, pool_id)
            if state == PoolState.RUNNING:
                return pool_id
        raise InferenceModelInternalError(
            f"No RUNNING pools available for model {model_id}"
        )

    def register_pool(self, model_id, pool_id, request_pool, request_queue):
        self.set_state(model_id, pool_id, PoolState.RUNNING)
        self.set_request_pool_map(model_id, pool_id, request_pool, request_queue)

    def add_request(self, pool_id, req):
        req_q = self.get_request_queue(req.model_id, pool_id)
        req_q.put(req)

    def remove_request(self, model_id, req_id):
        pass

    def get_pool_ids(self, model_id) -> list[int]:
        return self._request_pool_map[model_id].get_pool_ids()

    def has_request_pools(self, model_id) -> bool:
        return model_id in self._request_pool_map

    def get_request_pool_map(self) -> Dict[str, PoolGroup]:
        return self._request_pool_map

    def get_request_pools_group(self, model_id) -> Optional[PoolGroup]:
        return self._request_pool_map.get(model_id, None)

    def get_request_pool(self, model_id, pool_id) -> InferenceRequestPool:
        return self._request_pool_map[model_id].get_request_pool(pool_id)

    def get_request_queue(self, model_id, pool_id) -> mp.Queue:
        return self._request_pool_map[model_id].get_request_queue(pool_id)

    def set_request_pool_map(self, model_id, pool_id, request_pool, request_queue):
        if model_id not in self._request_pool_map:
            self._request_pool_map[model_id] = PoolGroup(model_id)
        self._request_pool_map[model_id].add_pool(pool_id, request_pool, request_queue)

    def get_state(self, model_id, pool_id) -> PoolState:
        return self.pool_states[model_id][pool_id]

    def set_state(self, model_id, pool_id, state):
        self.pool_states[model_id][pool_id] = state

    def get_load(self, model_id, pool_id) -> int:
        pass

    def shutdown(self):
        for model_id, pool_group in self._request_pool_map.items():
            for pool_id in pool_group.get_pool_ids():
                request_pool = pool_group.get_request_pool(pool_id)
                request_queue = pool_group.get_request_queue(pool_id)
                request_pool.stop()
                while not request_queue.empty():
                    request_queue.get_nowait()
                request_queue.close()
            for pool_id in pool_group.get_pool_ids():
                request_pool = pool_group.get_request_pool(pool_id)
                request_pool.join(timeout=10)
