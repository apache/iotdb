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

import random
import threading
import time

import numpy as np
import torch
import torch.multiprocessing as mp
from transformers import PretrainedConfig, PreTrainedModel

from ainode.core.config import AINodeDescriptor
from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.log import Logger
from ainode.core.manager.model_manager import ModelManager

logger = Logger()


class InferenceRequestPool(mp.Process):
    """
    The request pool to handle inference for a specific model.
    """

    FIX_SEED = 2021
    WAITING_INTERVAL_IN_MS = (
        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
    )  # How often to check for requests in the waiting/running queue

    def __init__(
        self,
        pool_id: int,
        model_id: int,
        config: PretrainedConfig,
        request_queue: mp.Queue,
        result_queue: mp.Queue,
        **pool_kwargs,
    ):
        super().__init__()
        self.pool_id = pool_id
        self.model_id = model_id
        self.config = config
        self.pool_kwargs = pool_kwargs
        self.model = None
        self.device = None

        # TODO: A scheduler is necessary for better handling following queues
        self._threads = []
        self._waiting_queue = request_queue  # Requests that are waiting to be processed
        self._running_queue = mp.Queue()  # Requests that are currently being processed
        self._finished_queue = result_queue  # Requests that are finished
        self._stop_event = mp.Event()

        # Fix inference seed
        random.seed(self.FIX_SEED)
        torch.manual_seed(self.FIX_SEED)
        np.random.seed(self.FIX_SEED)

    def memory_is_available(self, request):
        # need test with several rounds of dummy data
        pass

    def _activate_requests(self):
        if self._waiting_queue.empty():
            return
        request: InferenceRequest = self._waiting_queue.get()
        # TODO: Check memory size before activating requests
        request.inputs = request.inference_pipeline.preprocess_inputs(request.inputs)
        request.mark_running()
        logger.debug(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is activated with inputs shape {request.inputs.shape}"
        )
        self._running_queue.put(request)

    def _requests_activate_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._activate_requests()

    def _step(self):
        if self._running_queue.empty():
            return
        # TODO: We need a batcher to accelerate the concurrent inference
        # TODO: Check memory size before executing requests
        request: InferenceRequest = self._running_queue.get()
        inputs = request.inputs.to(self.device)
        output = self.model.generate(
            inputs,
            max_new_tokens=request.max_new_tokens,
            num_samples=10,
            revin=True,
        )
        request.output_tensor = request.output_tensor.to(
            self.device
        )  # Ensure output tensor is on the same device
        request.write_step_output(output[0].mean(dim=0))
        request.inference_pipeline.post_decode()
        if request.is_finished():
            request.inference_pipeline.post_inference()
            logger.debug(
                f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is finished"
            )
            # ensure the output tensor is on CPU before sending to result queue
            request.output_tensor = request.output_tensor.cpu()
            self._finished_queue.put(request)
        else:
            logger.debug(
                f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is not finished, re-queueing"
            )
            self._waiting_queue.put(request)

    def _requests_execute_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._step()

    def run(self):
        self._model_manager = ModelManager()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self._model_manager.load_model(self.model_id, {}).to(self.device)

        activate_daemon = threading.Thread(
            target=self._requests_activate_loop, daemon=True
        )
        self._threads.append(activate_daemon)
        activate_daemon.start()
        execute_daemon = threading.Thread(
            target=self._requests_execute_loop, daemon=True
        )
        self._threads.append(execute_daemon)
        execute_daemon.start()
        for thread in self._threads:
            thread.join()

    def stop(self):
        self._stop_event.set()
