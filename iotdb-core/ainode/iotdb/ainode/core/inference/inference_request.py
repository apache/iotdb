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
1import threading
1from typing import Any
1
1import torch
1
1from iotdb.ainode.core.inference.strategy.abstract_inference_pipeline import (
1    AbstractInferencePipeline,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.util.atmoic_int import AtomicInt
1
1logger = Logger()
1
1
1class InferenceRequestState:
1    WAITING = "waiting"
1    RUNNING = "running"
1    FINISHED = "finished"
1
1
1class InferenceRequest:
1    def __init__(
1        self,
1        req_id: str,
1        model_id: str,
1        inputs: torch.Tensor,
1        inference_pipeline: AbstractInferencePipeline,
1        max_new_tokens: int = 96,
1        **infer_kwargs,
1    ):
1        if inputs.ndim == 1:
1            inputs = inputs.unsqueeze(0)
1
1        self.req_id = req_id
1        self.model_id = model_id
1        self.inputs = inputs
1        self.infer_kwargs = infer_kwargs
1        self.inference_pipeline = inference_pipeline
1        self.max_new_tokens = (
1            max_new_tokens  # Number of time series data points to generate
1        )
1
1        self.batch_size = inputs.size(0)
1        self.state = InferenceRequestState.WAITING
1        self.cur_step_idx = 0  # Current write position in the output step index
1        self.assigned_pool_id = -1  # The pool handling this request
1        self.assigned_device_id = -1  # The device handling this request
1
1        # Preallocate output buffer [batch_size, max_new_tokens]
1        self.output_tensor = torch.zeros(
1            self.batch_size, max_new_tokens, device="cpu"
1        )  # shape: [self.batch_size, max_new_steps]
1
1    def mark_running(self):
1        self.state = InferenceRequestState.RUNNING
1
1    def mark_finished(self):
1        self.state = InferenceRequestState.FINISHED
1
1    def is_finished(self) -> bool:
1        return (
1            self.state == InferenceRequestState.FINISHED
1            or self.cur_step_idx >= self.max_new_tokens
1        )
1
1    def write_step_output(self, step_output: torch.Tensor):
1        if step_output.ndim == 1:
1            step_output = step_output.unsqueeze(0)
1
1        batch_size, step_size = step_output.shape
1        end_idx = self.cur_step_idx + step_size
1
1        if end_idx > self.max_new_tokens:
1            self.output_tensor[:, self.cur_step_idx :] = step_output[
1                :, : self.max_new_tokens - self.cur_step_idx
1            ]
1            self.cur_step_idx = self.max_new_tokens
1        else:
1            self.output_tensor[:, self.cur_step_idx : end_idx] = step_output
1            self.cur_step_idx = end_idx
1
1        if self.is_finished():
1            self.mark_finished()
1
1    def get_final_output(self) -> torch.Tensor:
1        return self.output_tensor[:, : self.cur_step_idx]
1
1
1class InferenceRequestProxy:
1    """
1    Wrap the raw request for handling multiprocess processing.
1    """
1
1    def __init__(self, req_id: str):
1        self.req_id = req_id
1        self.result = None
1        self._counter: AtomicInt = None
1        self._lock = threading.Lock()
1        self._condition = threading.Condition(self._lock)
1
1    def set_result(self, result: Any):
1        with self._lock:
1            self.result = result
1            if self._counter is not None:
1                self._counter.decrement_and_get()
1            self._condition.notify_all()
1
1    def set_counter(self, counter: AtomicInt):
1        with self._lock:
1            self._counter = counter
1
1    def wait_for_result(self) -> Any:
1        with self._lock:
1            self._condition.wait()
1            return self.result
1