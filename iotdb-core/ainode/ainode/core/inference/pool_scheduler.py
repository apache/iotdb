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

import torch
import torch.multiprocessing as mp

from ainode.core.exception import (
    InferenceModelInternalError,
)
from ainode.core.inference.inference_request_pool import InferenceRequestPool, PoolState
from ainode.core.inference.pool_controller import PoolController
from ainode.core.log import Logger
from ainode.core.manager.utils import (
    _estimate_pool_size,
)
from ainode.core.model.sundial.configuration_sundial import SundialConfig
from ainode.core.model.timerxl.configuration_timer import TimerConfig
from ainode.core.util.decorator import synchronized

logger = Logger()


class PoolScheduler:
    """
    A Scheduler to init the request pools.
    It initializes the first pool and starts a background thread to expand pools
    as needed based on the model_id.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, pool_controller: PoolController, result_queue: mp.Queue):
        self._pool_controller = pool_controller
        self._result_queue = result_queue

    @synchronized(threading.Lock())
    def first_req_init(self, model_id: str):
        if not self._pool_controller.has_request_pools(model_id):
            pool_num = _estimate_pool_size(self.DEFAULT_DEVICE, model_id)
            if pool_num <= 0:
                raise InferenceModelInternalError(
                    f"Not enough memory to run model {model_id}."
                )
            # initialize the first pool
            self._first_pool_init(model_id)
            # start a background thread to expand pools
            expand_thread = threading.Thread(
                target=self._expand_pools,
                args=(model_id, 1, pool_num - 1),
                daemon=True,
            )
            expand_thread.start()

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
        self._pool_controller.set_state(model_id, 0, PoolState.INITIALIZING)
        if not ready_event.wait(timeout=30):
            logger.error(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-0] First pool failed to be ready in time"
            )
        else:
            self._pool_controller.register_pool(model_id, 0, first_pool, first_queue)
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
            self._pool_controller.set_state(model_id, pool_id, PoolState.INITIALIZING)
            if not pool.ready_event.wait(timeout=30):
                logger.error(
                    f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_id}] Pool failed to be ready in time"
                )
                continue
            else:
                self._pool_controller.register_pool(model_id, pool_id, pool, queue)
                logger.info(
                    f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool.pool_id}] New inference request pool started for model {model_id}"
                )
