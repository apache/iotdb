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
from typing import Any

import torch

from ainode.core.inference.strategy.abstract_inference_pipeline import (
    AbstractInferencePipeline,
)
from ainode.core.log import Logger

logger = Logger()


class InferenceRequestState:
    WAITING = "waiting"
    RUNNING = "running"
    FINISHED = "finished"


class InferenceRequest:
    def __init__(
        self,
        req_id: str,
        inputs: torch.Tensor,
        inference_pipeline: AbstractInferencePipeline,
        max_new_tokens: int = 96,
        **infer_kwargs,
    ):
        if inputs.ndim == 1:
            inputs = inputs.unsqueeze(0)

        self.req_id = req_id
        self.inputs = inputs
        self.infer_kwargs = infer_kwargs
        self.inference_pipeline = inference_pipeline
        self.max_new_tokens = (
            max_new_tokens  # Number of time series data points to generate
        )

        self.batch_size = inputs.size(0)
        self.state = InferenceRequestState.WAITING
        self.cur_step_idx = 0  # Current write position in the output step index

        # Preallocate output buffer [batch_size, max_new_tokens]
        device = inputs.device
        self.output_tensor = torch.zeros(
            self.batch_size, max_new_tokens, device=device
        )  # shape: [self.batch_size, max_new_steps]

    def mark_running(self):
        self.state = InferenceRequestState.RUNNING

    def mark_finished(self):
        self.state = InferenceRequestState.FINISHED

    def is_finished(self) -> bool:
        return (
            self.state == InferenceRequestState.FINISHED
            or self.cur_step_idx >= self.max_new_tokens
        )

    def write_step_output(self, step_output: torch.Tensor):
        if step_output.ndim == 1:
            step_output = step_output.unsqueeze(0)

        batch_size, step_size = step_output.shape
        end_idx = self.cur_step_idx + step_size

        if end_idx > self.max_new_tokens:
            self.output_tensor[:, self.cur_step_idx :] = step_output[
                :, : self.max_new_tokens - self.cur_step_idx
            ]
            self.cur_step_idx = self.max_new_tokens
        else:
            self.output_tensor[:, self.cur_step_idx : end_idx] = step_output
            self.cur_step_idx = end_idx

        if self.is_finished():
            self.mark_finished()

    def get_final_output(self) -> torch.Tensor:
        return self.output_tensor[:, : self.cur_step_idx]


class InferenceRequestProxy:
    """
    Wrap the raw request for handling multiprocess processing.
    """

    def __init__(self, req_id: str):
        self.req_id = req_id
        self.result = None
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def set_result(self, result: Any):
        with self._lock:
            self.result = result
            self._condition.notify_all()

    def wait_for_completion(self) -> Any:
        with self._lock:
            self._condition.wait()
            return self.result
