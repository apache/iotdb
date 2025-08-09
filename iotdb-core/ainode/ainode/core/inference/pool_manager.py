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
import threading
from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.log import Logger
from ainode.core.exception import (
    InferenceModelInternalError,
)
from typing import Dict, Optional
import torch.multiprocessing as mp
from ainode.core.inference.inference_request_pool import InferenceRequestPool, PoolState
import torch
from ainode.core.inference.inference_request_pool_group import PoolGroup

logger = Logger()


class PoolManager:
    """
    A manager for handling inference request pools.
    It handles the registration of pools, adding and removing requests,
    and managing the state of each pool.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self):
        # structure: {model_id: {pool_id: set(req_id)}}
        self.pool_to_reqs: Dict[str, Dict[int, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        # structure: {req_id: pool_id}
        self.req_to_pool: Dict[str, int] = {}
        # structure: {model_id: {pool_id: PoolState}}
        self.pool_states: Dict[str, Dict[int, PoolState]] = defaultdict(dict)
        # structure: {model_id: {pool_id: threading.RLock}}
        self.pool_locks: Dict[str, Dict[int, threading.RLock]] = defaultdict(
            lambda: defaultdict(threading.RLock)
        )
        # structure: {model_id: {pool_id: PoolGroup}}
        self._request_pool_map: Dict[str, PoolGroup] = {}

    def dispatch_request(self, model_id, req: InferenceRequest):
        pool_idx = self._get_optimal_pool(model_id)
        if pool_idx is None:
            raise InferenceModelInternalError("No available pool for model")
        self.add_request(pool_idx, req)
        logger.debug(
            f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_idx}][ID-{req.req_id}] Request is queued for inference"
        )

    def _get_optimal_pool(self, model_id) -> int:
        loads = []
        for pool_idx in self.get_pool_ids(model_id):
            load_count = self.get_load(model_id, pool_idx)
            loads.append((pool_idx, load_count))
        min_idx = min(loads, key=lambda x: x[1])[0]
        return min_idx

    def register_pool(self, model_id, pool_id, request_pool, request_queue):
        self.set_state(model_id, pool_id, PoolState.RUNNING)
        self.set_request_pool_map(model_id, pool_id, request_pool, request_queue)

    def add_request(self, pool_id, req):
        model_id = req.model_id
        with self.get_lock(model_id, pool_id):
            cur_state = self.get_state(model_id, pool_id)
            if cur_state == PoolState.RUNNING:
                req_q = self.get_request_queue(model_id, pool_id)
                req_q.put(req)
            else:
                raise InferenceModelInternalError(
                    f"Pool {pool_id} for model {model_id} is not in a valid state: {cur_state}"
                )
            self.pool_to_reqs[model_id][pool_id].add(req.req_id)
            self.req_to_pool[req.req_id] = pool_id

    def remove_request(self, model_id, req_id):
        pool_id = self.get_pool_for_req(req_id)
        with self.get_lock(model_id, pool_id):
            self.pool_to_reqs[model_id][pool_id].discard(req_id)
            del self.req_to_pool[req_id]

    def get_pool_ids(self, model_id) -> list[int]:
        return self._request_pool_map[model_id].get_pool_ids()

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
        with self.get_lock(model_id, pool_id):
            return self.pool_states[model_id][pool_id]

    def set_state(self, model_id, pool_id, state):
        with self.get_lock(model_id, pool_id):
            self.pool_states[model_id][pool_id] = state

    def get_load(self, model_id, pool_id) -> int:
        with self.get_lock(model_id, pool_id):
            return len(self.pool_to_reqs[model_id][pool_id])

    def get_pool_for_req(self, req_id) -> int:
        return self.req_to_pool.get(req_id)

    def get_active_requests(self, model_id, pool_id) -> set[int]:
        with self.get_lock(model_id, pool_id):
            return self.pool_to_reqs[model_id][pool_id]

    def get_lock(self, model_id, pool_id) -> threading.RLock:
        return self.pool_locks[model_id][pool_id]

    def shutdown(self):
        for model_id, pool_group in self._request_pool_map.items():
            for pool_id in pool_group.get_pool_ids():
                requestPool = pool_group.get_request_pool(pool_id)
                requestQueue = pool_group.get_request_queue(pool_id)
                active_requests = self.get_active_requests(model_id, pool_id)
                if active_requests:
                    logger.warning(
                        f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_id}] "
                        f"Shutting down Pool-{pool_id} with {len(active_requests)} active request(s): {list(active_requests)}"
                    )
                requestPool.stop()
                while not requestQueue.empty():
                    requestQueue.get_nowait()
                requestQueue.close()
            for pool_id in pool_group.get_pool_ids():
                requestPool = pool_group.get_request_pool(pool_id)
                requestPool.join(timeout=10)
