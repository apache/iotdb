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
import time

import torch.multiprocessing as mp

from ainode.core.config import AINodeDescriptor
from ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from ainode.core.inference.pool_controller import PoolController
from ainode.core.inference.pool_scheduler import PoolScheduler
from ainode.core.log import Logger

logger = Logger()


class RequestController:
    """
    Controls the lifecycle and scheduling of inference requests.
    """

    WAITING_INTERVAL_IN_MS = (
        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
    )  # How often to check for requests in the result queue

    def __init__(self):
        self._result_queue = mp.Queue()
        self._result_wrapper_map = {}
        self._result_wrapper_lock = threading.RLock()

        self._stop_event = mp.Event()
        self._result_handler_thread = threading.Thread(
            target=self._handle_results, daemon=True
        )
        self._result_handler_thread.start()
        self._pool_controller = PoolController()
        self._pool_scheduler = PoolScheduler(self._pool_controller, self._result_queue)

    def _handle_results(self):
        while not self._stop_event.is_set():
            if self._result_queue.empty():
                time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
                continue
            infer_req: InferenceRequest = self._result_queue.get()
            self._pool_controller.remove_request(infer_req.model_id, infer_req.req_id)
            with self._result_wrapper_lock:
                self._result_wrapper_map[infer_req.req_id].set_result(
                    infer_req.get_final_output()
                )

    def process_request(self, req):
        infer_proxy = InferenceRequestProxy(req.req_id)
        with self._result_wrapper_lock:
            self._result_wrapper_map[req.req_id] = infer_proxy
        # lazy initialization for first request
        model_id = req.model_id
        if not self._pool_controller.has_request_pools(model_id):
            self._pool_scheduler.first_req_init(model_id)
        # dispatch request to the pool
        self._pool_controller.dispatch_request(model_id, req)
        outputs = infer_proxy.wait_for_completion()
        with self._result_wrapper_lock:
            del self._result_wrapper_map[req.req_id]
        return outputs

    def shutdown(self):
        self._stop_event.set()
        self._pool_controller.shutdown()
        while not self._result_queue.empty():
            self._result_queue.get_nowait()
        self._result_queue.close()
