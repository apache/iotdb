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

import torch.multiprocessing as mp

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.dispatcher.basic_dispatcher import BasicDispatcher
from iotdb.ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from iotdb.ainode.core.inference.inference_request_pool import (
    InferenceRequestPool,
    PoolState,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.util.atmoic_int import AtomicInt

logger = Logger()


class PoolGroup:
    """
    A group of inference request pools for a specific model.
    """

    def __init__(self, model_id):
        # structure: {pool_id: (InferenceRequestPool, waitingQueue)}
        self.pool_group: Dict[int, Tuple[InferenceRequestPool, mp.Queue]] = {}
        # structure: {pool_id: PoolState}
        self.pool_states: Dict[int, PoolState] = {}
        # structure: {pool_id: remaining_reqs_cnt}
        self.pool_remaining_reqs: Dict[int, AtomicInt] = {}
        self.model_id = model_id
        self.request_dispatcher = BasicDispatcher(self.pool_states)

    def get_pool_group(self) -> Dict[int, Tuple[InferenceRequestPool, mp.Queue]]:
        return self.pool_group

    def add_pool(
        self, pool_id: int, request_pool: InferenceRequestPool, request_queue: mp.Queue
    ):
        self.pool_group[pool_id] = (request_pool, request_queue)
        self.pool_remaining_reqs[pool_id] = AtomicInt()

    def remove_pool(self, pool_id: int):
        self.pool_group.pop(pool_id, None)
        self.pool_states.pop(pool_id, None)
        self.pool_remaining_reqs.pop(pool_id, None)

    def get_pool_ids(self) -> list[int]:
        return list(self.pool_group.keys())

    def get_pool_count(self) -> int:
        return len(self.pool_group)

    def get_running_pool_count(self) -> int:
        count = 0
        for _, state in self.pool_states.items():
            count += 1 if state == PoolState.RUNNING else 0
        return count

    def dispatch_request(
        self, req: InferenceRequest, infer_proxy: InferenceRequestProxy
    ):
        pool_id = self.request_dispatcher.dispatch_request(req, self.get_pool_ids())
        req_q = self.pool_group[pool_id][1]
        self.pool_remaining_reqs[pool_id].increment_and_get()
        infer_proxy.set_counter(self.pool_remaining_reqs[pool_id])
        req_q.put(req)
        logger.debug(
            f"[Inference][Pool-{pool_id}][Req-{req.req_id}] Request is queued for inference"
        )

    def get_request_pool(self, pool_id) -> InferenceRequestPool:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalException(
                f"[Inference][Pool-{pool_id}] Pool not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][0]

    def get_request_queue(self, pool_id) -> mp.Queue:
        if pool_id not in self.pool_group:
            raise InferenceModelInternalException(
                f"[Inference][Pool-{pool_id}] Pool not found for model {self.model_id}"
            )
        return self.pool_group[pool_id][1]

    def get_state(self, pool_id) -> PoolState:
        return self.pool_states[pool_id]

    def set_state(self, pool_id, state):
        self.pool_states[pool_id] = state

    def get_load(self, pool_id) -> int:
        """
        Currently, we use the number of remaining requests in the pool as the load estimation.
        TODO: we will implement better load estimation in the future.
        """
        return self.pool_remaining_reqs[pool_id].get()

    def shutdown(self):
        for pool_id, (request_pool, _) in self.pool_group.items():
            logger.info(f"[Inference][Pool-{pool_id}] Shutting down the pool...")
            request_pool.stop()
