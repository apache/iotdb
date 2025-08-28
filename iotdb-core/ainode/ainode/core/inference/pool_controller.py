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

import threading
from typing import Dict, Optional

import torch
import torch.multiprocessing as mp

from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.inference.inference_request_pool import InferenceRequestPool, PoolState
from ainode.core.inference.pool_group import PoolGroup
from ainode.core.inference.pool_scheduler.basic_pool_scheduler import (
    BasicPoolScheduler,
    ScaleActionType,
)
from ainode.core.log import Logger
from ainode.core.model.sundial.configuration_sundial import SundialConfig
from ainode.core.model.timerxl.configuration_timer import TimerConfig
from ainode.core.util.decorator import synchronized

logger = Logger()


class PoolController:
    """
    A controller for handling inference request pools.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, result_queue: mp.Queue):
        # structure: {model_id: PoolGroup}
        self._request_pool_map: Dict[str, PoolGroup] = {}
        self._result_queue = result_queue
        self._pool_scheduler = BasicPoolScheduler(self._request_pool_map)

    @synchronized(threading.Lock())
    def first_req_init(self, model_id: str):
        if not self.has_request_pools(model_id):
            actions = self._pool_scheduler.schedule(model_id)
            for action in actions:
                if action.action == ScaleActionType.SCALE_UP:
                    # initialize the first pool
                    self._first_pool_init(action.model_id)
                    # start a background thread to expand pools
                    expand_thread = threading.Thread(
                        target=self._expand_pools,
                        args=(action.model_id, 1, action.amount - 1),
                        daemon=True,
                    )
                    expand_thread.start()
                elif action.action == ScaleActionType.SCALE_DOWN:
                    # TODO: implement scale down logic
                    pass

    def _first_pool_init(self, model_id: str):
        if model_id == "sundial":
            config = SundialConfig()
        elif model_id == "timer_xl":
            config = TimerConfig()
        first_queue = mp.Queue()
        ready_event = mp.Event()
        first_pool = InferenceRequestPool(
            pool_id=0,
            model_id=model_id,
            config=config,
            request_queue=first_queue,
            result_queue=self._result_queue,
            ready_event=ready_event,
        )
        first_pool.start()
        self.register_pool(model_id, 0, first_pool, first_queue)
        if not ready_event.wait(timeout=30):
            self.unregister_pool(model_id, 0)
            logger.error(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-0] First pool failed to be ready in time"
            )
        else:
            self.set_state(model_id, 0, PoolState.RUNNING)
            logger.info(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-0] Initialized inference request pool for model {model_id}"
            )

    def _expand_pools(self, model_id, start_idx, count):
        for idx in range(count):
            queue = mp.Queue()
            pool_id = start_idx + idx
            if model_id == "sundial":
                config = SundialConfig()
            elif model_id == "timer_xl":
                config = TimerConfig()
            pool = InferenceRequestPool(
                pool_id=pool_id,
                model_id=model_id,
                config=config,
                request_queue=queue,
                result_queue=self._result_queue,
                ready_event=mp.Event(),
            )
            pool.start()
            self.register_pool(model_id, pool_id, pool, queue)
            if not pool.ready_event.wait(timeout=30):
                logger.error(
                    f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_id}] Pool failed to be ready in time"
                )
                self.unregister_pool(model_id, pool_id)
                continue
            else:
                self.set_state(model_id, pool_id, PoolState.RUNNING)
                logger.info(
                    f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool.pool_id}] New inference request pool started for model {model_id}"
                )

    def add_request(self, model_id: str, req: InferenceRequest):
        # lazy initialization for first request
        model_id = req.model_id
        if not self.has_request_pools(model_id):
            self.first_req_init(model_id)
        self._request_pool_map[model_id].dispatch_request(req)

    def get_state(self, model_id, pool_id) -> PoolState:
        return self._request_pool_map[model_id].get_state(pool_id)

    def set_state(self, model_id, pool_id, state):
        self._request_pool_map[model_id].set_state(pool_id, state)

    def register_pool(self, model_id, pool_id, request_pool, request_queue):
        self.set_request_pool_map(model_id, pool_id, request_pool, request_queue)
        pool_group = self._request_pool_map.get(model_id)
        pool_group.set_state(pool_id, PoolState.INITIALIZING)

    def unregister_pool(self, model_id, pool_id):
        self._request_pool_map[model_id].remove_pool(pool_id)
        if not self._request_pool_map[model_id].get_pool_ids():
            self._request_pool_map.pop(model_id, None)

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

    def shutdown(self):
        for pool_group in self._request_pool_map.values():
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
