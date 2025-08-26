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
from typing import Dict, Tuple

import torch
import torch.multiprocessing as mp

from ainode.core.exception import (
    InferenceModelInternalError,
)
from ainode.core.inference.dispatcher.basic_dispatcher import BasicDispatcher
from ainode.core.inference.inference_request_pool import InferenceRequestPool, PoolState
from ainode.core.log import Logger
from ainode.core.util.atmoic_int import AtomicInt

logger = Logger()


class PoolGroup:
    """
    A group of inference request pools for a specific model.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, model_id):
        # structure: {pool_id: (InferenceRequestPool, mp.Queue)}
        self.pool_group: Dict[int, Tuple[InferenceRequestPool, mp.Queue]] = {}
        # structure: {pool_id: PoolState}
        self.pool_states: Dict[int, PoolState] = {}
        # structure: {pool_id: current_load}
        self._loads: Dict[int, AtomicInt] = {}
        self.model_id = model_id
        self.request_dispatcher = BasicDispatcher(self.pool_states)

    def get_pool_group(self) -> Dict[int, Tuple[InferenceRequestPool, mp.Queue]]:
        return self.pool_group

    def add_pool(
        self, pool_id: int, request_pool: InferenceRequestPool, request_queue: mp.Queue
    ):
        self.pool_group[pool_id] = (request_pool, request_queue)
        self._loads[pool_id] = AtomicInt(0)

    def remove_pool(self, pool_id: int):
        self.pool_group.pop(pool_id, None)
        self.pool_states.pop(pool_id, None)
        self._loads.pop(pool_id, None)

    def get_pool_ids(self) -> list[int]:
        return list(self.pool_group.keys())

    def dispatch_request(self, req):
        pool_idx = self.request_dispatcher.dispatch_request(req, self.get_pool_ids())
        req.assigned_pool_id = pool_idx
        self._loads[pool_idx].incr()
        req_q = self.pool_group[pool_idx][1]
        req_q.put(req)
        logger.debug(
            f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_idx}][ID-{req.req_id}] Request is queued for inference"
        )

    def remove_request(self, pool_id: int):
        """
        remove the request from the pool's load count
        """
        if pool_id in self._loads:
            self._loads[pool_id].decr()

    def get_request_pool(self, pool_id) -> InferenceRequestPool:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalError(
                f"Pool ID {pool_id} not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][0]

    def get_request_queue(self, pool_id) -> mp.Queue:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalError(
                f"Pool ID {pool_id} not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][1]

    def get_state(self, pool_id) -> PoolState:
        return self.pool_states[pool_id]

    def set_state(self, pool_id, state):
        self.pool_states[pool_id] = state

    def get_load(self, pool_id) -> int:
        if pool_id not in self._loads:
            raise InferenceModelInternalError(
                f"Pool ID {pool_id} not found for model {self.model_id}"
            )
        return self._loads[pool_id].get()
