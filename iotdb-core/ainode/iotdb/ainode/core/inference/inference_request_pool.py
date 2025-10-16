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
1
1import random
1import threading
1import time
1from collections import defaultdict
1from enum import Enum
1
1import numpy as np
1import torch
1import torch.multiprocessing as mp
1from transformers import PretrainedConfig
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import INFERENCE_LOG_FILE_NAME_PREFIX_TEMPLATE
1from iotdb.ainode.core.inference.batcher.basic_batcher import BasicBatcher
1from iotdb.ainode.core.inference.inference_request import InferenceRequest
1from iotdb.ainode.core.inference.request_scheduler.basic_request_scheduler import (
1    BasicRequestScheduler,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.manager.model_manager import ModelManager
1from iotdb.ainode.core.util.gpu_mapping import convert_device_id_to_torch_device
1
1
1class PoolState(Enum):
1    INITIALIZING = "INITIALIZING"
1    RUNNING = "RUNNING"
1    STOPPING = "STOPPING"
1
1
1class InferenceRequestPool(mp.Process):
1    """
1    The request pool to handle inference for a specific model.
1    """
1
1    FIX_SEED = 2021
1    WAITING_INTERVAL_IN_MS = (
1        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
1    )  # How often to check for requests in the waiting/running queue
1
1    def __init__(
1        self,
1        pool_id: int,
1        model_id: str,
1        device: str,
1        config: PretrainedConfig,
1        request_queue: mp.Queue,
1        result_queue: mp.Queue,
1        ready_event,
1        **pool_kwargs,
1    ):
1        super().__init__()
1        self.pool_id = pool_id
1        self.model_id = model_id
1        self.config = config
1        self.pool_kwargs = pool_kwargs
1        self.ready_event = ready_event
1        self.device = convert_device_id_to_torch_device(device)
1
1        self._threads = []
1        self._waiting_queue = request_queue  # Requests that are waiting to be processed
1        self._running_queue = mp.Queue()  # Requests that are currently being processed
1        self._finished_queue = result_queue  # Requests that are finished
1        self._request_scheduler = BasicRequestScheduler(
1            self._waiting_queue, self._running_queue, self._finished_queue, self.pool_id
1        )
1        self._batcher = BasicBatcher()
1        self._stop_event = mp.Event()
1
1        self._model = None
1        self._model_manager = None
1        self._logger = None
1
1        # Fix inference seed
1        random.seed(self.FIX_SEED)
1        torch.manual_seed(self.FIX_SEED)
1        np.random.seed(self.FIX_SEED)
1
1    def _activate_requests(self):
1        requests = self._request_scheduler.schedule_activate()
1        for request in requests:
1            request.inputs = request.inference_pipeline.preprocess_inputs(
1                request.inputs
1            )
1            request.mark_running()
1            self._running_queue.put(request)
1            self._logger.debug(
1                f"[Inference][Device-{self.device}][Pool-{self.pool_id}][Req-{request.req_id}] Request is activated with inputs shape {request.inputs.shape}"
1            )
1
1    def _requests_activate_loop(self):
1        while not self._stop_event.is_set():
1            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
1            self._activate_requests()
1
1    def _step(self):
1        all_requests: list[InferenceRequest] = self._request_scheduler.schedule_step()
1
1        grouped_requests = defaultdict(list)
1        for req in all_requests:
1            key = (req.inputs.shape[1], req.max_new_tokens)
1            grouped_requests[key].append(req)
1        grouped_requests = list(grouped_requests.values())
1
1        for requests in grouped_requests:
1            batch_inputs = self._batcher.batch_request(requests).to(self.device)
1            if self.model_id == "sundial":
1                batch_output = self._model.generate(
1                    batch_inputs,
1                    max_new_tokens=requests[0].max_new_tokens,
1                    num_samples=10,
1                    revin=True,
1                )
1
1                offset = 0
1                for request in requests:
1                    request.output_tensor = request.output_tensor.to(self.device)
1                    cur_batch_size = request.batch_size
1                    cur_output = batch_output[offset : offset + cur_batch_size]
1                    offset += cur_batch_size
1                    # TODO Here we only considered the case where batchsize=1 in one request. If multi-variable adaptation is required in the future, modifications may be needed here, such as: `cur_output[0]` maybe not true in multi-variable scene
1                    request.write_step_output(cur_output[0].mean(dim=0))
1
1                    request.inference_pipeline.post_decode()
1                    if request.is_finished():
1                        request.inference_pipeline.post_inference()
1                        self._logger.debug(
1                            f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is finished"
1                        )
1                        # ensure the output tensor is on CPU before sending to result queue
1                        request.output_tensor = request.output_tensor.cpu()
1                        self._finished_queue.put(request)
1                    else:
1                        self._logger.debug(
1                            f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is not finished, re-queueing"
1                        )
1                        self._waiting_queue.put(request)
1
1            elif self.model_id == "timer_xl":
1                batch_output = self._model.generate(
1                    batch_inputs,
1                    max_new_tokens=requests[0].max_new_tokens,
1                    revin=True,
1                )
1
1                offset = 0
1                for request in requests:
1                    request.output_tensor = request.output_tensor.to(self.device)
1                    cur_batch_size = request.batch_size
1                    cur_output = batch_output[offset : offset + cur_batch_size]
1                    offset += cur_batch_size
1                    request.write_step_output(cur_output)
1
1                    request.inference_pipeline.post_decode()
1                    if request.is_finished():
1                        request.inference_pipeline.post_inference()
1                        self._logger.debug(
1                            f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is finished"
1                        )
1                        # ensure the output tensor is on CPU before sending to result queue
1                        request.output_tensor = request.output_tensor.cpu()
1                        self._finished_queue.put(request)
1                    else:
1                        self._logger.debug(
1                            f"[Inference][Device-{self.device}][Pool-{self.pool_id}][ID-{request.req_id}] Request is not finished, re-queueing"
1                        )
1                        self._waiting_queue.put(request)
1
1    def _requests_execute_loop(self):
1        while not self._stop_event.is_set():
1            time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
1            self._step()
1
1    def run(self):
1        self._logger = Logger(
1            INFERENCE_LOG_FILE_NAME_PREFIX_TEMPLATE.format(self.device)
1        )
1        self._model_manager = ModelManager()
1        self._request_scheduler.device = self.device
1        self._model = self._model_manager.load_model(self.model_id, {}).to(self.device)
1        self.ready_event.set()
1
1        activate_daemon = threading.Thread(
1            target=self._requests_activate_loop, daemon=True
1        )
1        self._threads.append(activate_daemon)
1        activate_daemon.start()
1        execute_daemon = threading.Thread(
1            target=self._requests_execute_loop, daemon=True
1        )
1        self._threads.append(execute_daemon)
1        execute_daemon.start()
1        for thread in self._threads:
1            thread.join()
1        self._logger.info(
1            f"[Inference][Device-{self.device}][Pool-{self.pool_id}] InferenceRequestPool for model {self.model_id} exited cleanly."
1        )
1
1    def stop(self):
1        self._stop_event.set()
1