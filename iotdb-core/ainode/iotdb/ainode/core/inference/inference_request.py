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

from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.util.atmoic_int import AtomicInt

logger = Logger()


class InferenceRequestState:
    WAITING = "waiting"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


def _normalize_targets_for_batch(targets: torch.Tensor) -> torch.Tensor:
    if targets.ndim == 1:
        return targets.unsqueeze(0)
    return targets


class InferenceRequest:
    def __init__(
        self,
        req_id: str,
        model_id: str,
        inputs: torch.Tensor | None = None,
        output_length: int = 96,
        model_inputs: list[dict[str, Any]] | None = None,
        inference_attrs: dict[str, Any] | None = None,
        **infer_kwargs,
    ):
        self.model_inputs = self._init_model_inputs(inputs, model_inputs)

        if inputs is None:
            inputs = torch.stack(
                [
                    _normalize_targets_for_batch(data["targets"])
                    for data in self.model_inputs
                ],
                dim=0,
            )

        while inputs.ndim < 3:
            inputs = inputs.unsqueeze(0)

        self.req_id = req_id
        self.model_id = model_id
        self.inputs = inputs
        self.inference_attrs = dict(inference_attrs or {})
        self.inference_attrs.update(infer_kwargs)
        if "output_length" not in self.inference_attrs:
            self.inference_attrs["output_length"] = output_length
        self.infer_kwargs = self.inference_attrs
        self.output_length = int(
            self.inference_attrs.get("output_length", output_length)
        )

        self.batch_size = inputs.size(0)
        self.target_count = inputs.size(1)
        self.input_length = inputs.size(2)
        self.state = InferenceRequestState.WAITING
        self.cur_step_idx = 0  # Current write position in the output step index
        self.assigned_pool_id = -1  # The pool handling this request
        self.assigned_device_id = -1  # The device handling this request
        self.error: str | None = None

        # Preallocate output buffer [batch_size, target_count, output_length]
        self.output_tensor = torch.zeros(
            self.batch_size, self.target_count, self.output_length, device="cpu"
        )

    @staticmethod
    def _init_model_inputs(
        inputs: torch.Tensor | None, model_inputs: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        if model_inputs is not None:
            if not model_inputs:
                raise ValueError("model_inputs must not be empty.")
            return model_inputs
        if inputs is None:
            raise ValueError("Either inputs or model_inputs must be provided.")

        while inputs.ndim < 3:
            inputs = inputs.unsqueeze(0)
        return [{"targets": inputs[i]} for i in range(inputs.size(0))]

    def mark_running(self):
        self.state = InferenceRequestState.RUNNING

    def mark_finished(self):
        self.state = InferenceRequestState.FINISHED

    def mark_failed(self, error: str):
        self.error = error
        self.state = InferenceRequestState.FAILED

    def has_covariates(self) -> bool:
        return any(
            set(model_input.keys()) - {"targets"} for model_input in self.model_inputs
        )

    def is_finished(self) -> bool:
        return (
            self.state == InferenceRequestState.FINISHED
            or self.state == InferenceRequestState.FAILED
            or self.cur_step_idx >= self.output_length
        )

    def write_step_output(self, step_output: torch.Tensor):
        while step_output.ndim < 3:
            step_output = step_output.unsqueeze(0)

        batch_size, target_count, step_size = step_output.shape
        end_idx = self.cur_step_idx + step_size

        if end_idx > self.output_length:
            self.output_tensor[:, :, self.cur_step_idx :] = step_output[
                :, :, : self.output_length - self.cur_step_idx
            ]
            self.cur_step_idx = self.output_length
        else:
            self.output_tensor[:, :, self.cur_step_idx : end_idx] = step_output
            self.cur_step_idx = end_idx

        if self.is_finished():
            self.mark_finished()

    def get_final_output(self) -> torch.Tensor:
        return self.output_tensor[:, :, : self.cur_step_idx]


class InferenceRequestProxy:
    """
    Wrap the raw request for handling multiprocess processing.
    """

    def __init__(self, req_id: str):
        self.req_id = req_id
        self.result = None
        self.exception = None
        self._done = False
        self._counter: AtomicInt = None
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def set_result(self, result: Any):
        with self._lock:
            self.result = result
            self._done = True
            if self._counter is not None:
                self._counter.decrement_and_get()
            self._condition.notify_all()

    def set_exception(self, exception: Exception):
        with self._lock:
            self.exception = exception
            self._done = True
            if self._counter is not None:
                self._counter.decrement_and_get()
            self._condition.notify_all()

    def set_counter(self, counter: AtomicInt):
        with self._lock:
            self._counter = counter

    def wait_for_result(self) -> Any:
        with self._lock:
            while not self._done:
                self._condition.wait()
            if self.exception is not None:
                raise self.exception
            return self.result
