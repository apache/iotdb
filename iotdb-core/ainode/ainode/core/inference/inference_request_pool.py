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

import gc
import random
import threading
import time

import numpy as np
import torch
import torch.multiprocessing as mp
from transformers import PretrainedConfig

from ainode.core.config import AINodeDescriptor
from ainode.core.inference.inference_request import InferenceRequest
from ainode.core.inference.scheduler.basic_scheduler import BasicScheduler
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
        model_id: str,
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
        self._model_manager = None
        self.device = None

        self._threads = []
        self._waiting_queue = request_queue  # Requests that are waiting to be processed
        self._running_queue = mp.Queue()  # Requests that are currently being processed
        self._finished_queue = result_queue  # Requests that are finished
        self._scheduler = BasicScheduler(
            self._waiting_queue, self._running_queue, self._finished_queue, self.pool_id
        )
        self._stop_event = mp.Event()

        # Fix inference seed
        random.seed(self.FIX_SEED)
        torch.manual_seed(self.FIX_SEED)
        np.random.seed(self.FIX_SEED)

    def _warm_up_and_estimate_memory(self):
        # TODO: Test per token memory usage, add support for cpu in the future
        torch.cuda.empty_cache()
        gc.collect()
        dummy_input = torch.zeros(
            (1, self.config.input_token_len), dtype=torch.float32
        ).to(self.device)

        # force cuda synchronization to avoid any asynchronous memory allocation issues
        torch.cuda.reset_peak_memory_stats(self.device)
        torch.cuda.synchronize(self.device)
        memory_before_warmup = torch.cuda.memory_allocated(self.device)
        logger.info(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] Before warm-up, peak memory usage: {memory_before_warmup:.2f} bytes"
        )

        # warm-up
        with torch.no_grad():
            self.model.generate(dummy_input, max_new_tokens=1)
        torch.cuda.synchronize(self.device)
        peak_memory_1_token = torch.cuda.max_memory_allocated(self.device)
        logger.info(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] Baseline memory usage for 1 token: {peak_memory_1_token:.2f} bytes"
        )
        logger.info(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] Differentiation : {peak_memory_1_token-memory_before_warmup:.2f} bytes"
        )

    def _activate_requests(self):
        requests = self._scheduler.schedule_activate()
        for request in requests:
            request.inputs = request.inference_pipeline.preprocess_inputs(
                request.inputs
            )
            request.mark_running()
            self._running_queue.put(request)
            logger.debug(
                f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is activated with inputs shape {request.inputs.shape}"
            )

    def _requests_activate_loop(self):
        while not self._stop_event.is_set():
            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
            self._activate_requests()

    def _step(self):
        requests = self._scheduler.schedule_step()
        # TODO: We need a batcher to accelerate the concurrent inference
        for request in requests:
            request.inputs = request.inputs.to(self.device)
            output = self.model.generate(
                request.inputs,
                max_new_tokens=request.max_new_tokens,
                num_samples=10,
                revin=True,
            )
            request.output_tensor = request.output_tensor.to(self.device)
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
        self._scheduler.device = self.device
        self.model = self._model_manager.load_model(self.model_id, {}).to(self.device)

        # self._warm_up_and_estimate_memory()

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
        logger.info(
            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] Stopping and releasing resources."
        )
        try:
            del self.model
            if "cuda" in str(self.device):
                torch.cuda.empty_cache()
            gc.collect()
        except Exception as e:
            logger.warning(
                f"[Inference][Device-{self.device}][Pool-{self.pool_id}] Failed to clean up: {e}"
            )
