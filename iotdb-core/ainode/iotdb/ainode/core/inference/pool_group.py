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
1from typing import Dict, Tuple
1
1import torch.multiprocessing as mp
1
1from iotdb.ainode.core.exception import InferenceModelInternalError
1from iotdb.ainode.core.inference.dispatcher.basic_dispatcher import BasicDispatcher
1from iotdb.ainode.core.inference.inference_request import (
1    InferenceRequest,
1    InferenceRequestProxy,
1)
1from iotdb.ainode.core.inference.inference_request_pool import (
1    InferenceRequestPool,
1    PoolState,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.util.atmoic_int import AtomicInt
1
1logger = Logger()
1
1
1class PoolGroup:
1    """
1    A group of inference request pools for a specific model.
1    """
1
1    def __init__(self, model_id):
1        # structure: {pool_id: (InferenceRequestPool, waitingQueue)}
1        self.pool_group: Dict[int, Tuple[InferenceRequestPool, mp.Queue]] = {}
1        # structure: {pool_id: PoolState}
1        self.pool_states: Dict[int, PoolState] = {}
1        # structure: {pool_id: remaining_reqs_cnt}
1        self.pool_remaining_reqs: Dict[int, AtomicInt] = {}
1        self.model_id = model_id
1        self.request_dispatcher = BasicDispatcher(self.pool_states)
1
1    def get_pool_group(self) -> Dict[int, Tuple[InferenceRequestPool, mp.Queue]]:
1        return self.pool_group
1
1    def add_pool(
1        self, pool_id: int, request_pool: InferenceRequestPool, request_queue: mp.Queue
1    ):
1        self.pool_group[pool_id] = (request_pool, request_queue)
1        self.pool_remaining_reqs[pool_id] = AtomicInt()
1
1    def remove_pool(self, pool_id: int):
1        self.pool_group.pop(pool_id, None)
1        self.pool_states.pop(pool_id, None)
1        self.pool_remaining_reqs.pop(pool_id, None)
1
1    def get_pool_ids(self) -> list[int]:
1        return list(self.pool_group.keys())
1
1    def get_pool_count(self) -> int:
1        return len(self.pool_group)
1
1    def dispatch_request(
1        self, req: InferenceRequest, infer_proxy: InferenceRequestProxy
1    ):
1        pool_id = self.request_dispatcher.dispatch_request(req, self.get_pool_ids())
1        req_q = self.pool_group[pool_id][1]
1        self.pool_remaining_reqs[pool_id].increment_and_get()
1        infer_proxy.set_counter(self.pool_remaining_reqs[pool_id])
1        req_q.put(req)
1        logger.debug(
1            f"[Inference][Pool-{pool_id}][Req-{req.req_id}] Request is queued for inference"
1        )
1
1    def get_request_pool(self, pool_id) -> InferenceRequestPool:
1        if pool_id not in self.pool_group:
1            raise InferenceModelInternalError(
1                f"[Inference][Pool-{pool_id}] Pool not found for model {self.model_id}"
1            )
1        return self.pool_group[pool_id][0]
1
1    def get_request_queue(self, pool_id) -> mp.Queue:
1        if pool_id not in self.pool_group:
1            raise InferenceModelInternalError(
1                f"[Inference][Pool-{pool_id}] Pool not found for model {self.model_id}"
1            )
1        return self.pool_group[pool_id][1]
1
1    def get_state(self, pool_id) -> PoolState:
1        return self.pool_states[pool_id]
1
1    def set_state(self, pool_id, state):
1        self.pool_states[pool_id] = state
1
1    def get_load(self, pool_id) -> int:
1        """
1        Currently, we use the number of remaining requests in the pool as the load estimation.
1        TODO: we will implement better load estimation in the future.
1        """
1        return self.pool_remaining_reqs[pool_id].get()
1
1    def shutdown(self):
1        for pool_id, (request_pool, _) in self.pool_group.items():
1            logger.info(f"[Inference][Pool-{pool_id}] Shutting down the pool...")
1            request_pool.stop()
1