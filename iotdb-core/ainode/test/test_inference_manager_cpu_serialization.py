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
from types import SimpleNamespace

import torch

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline
from iotdb.ainode.core.manager import inference_manager as inference_manager_module
from iotdb.ainode.core.manager.inference_manager import InferenceManager


class _SerialTestForecastPipeline(ForecastPipeline):
    def __init__(self):
        pass

    def preprocess(self, inputs, **infer_kwargs):
        return inputs

    def forecast(self, inputs, **infer_kwargs):
        return [torch.tensor([1.0])]

    def postprocess(self, outputs, **infer_kwargs):
        return outputs


def test_cpu_non_pool_same_model_requests_are_serialized(monkeypatch):
    manager = InferenceManager()
    active_loads = 0
    max_active_loads = 0
    active_lock = threading.Lock()
    start_barrier = threading.Barrier(6)
    errors = []

    def fake_load_pipeline(model_info, device, **model_kwargs):
        nonlocal active_loads, max_active_loads
        with active_lock:
            active_loads += 1
            max_active_loads = max(max_active_loads, active_loads)
        try:
            time.sleep(0.05)
            return _SerialTestForecastPipeline()
        finally:
            with active_lock:
                active_loads -= 1

    def run_request():
        try:
            start_barrier.wait(timeout=5)
            manager._run_inference_without_pool(
                "same_model",
                [{"targets": torch.tensor([1.0])}],
                {},
            )
        except Exception as error:
            errors.append(error)

    monkeypatch.setattr(inference_manager_module, "load_pipeline", fake_load_pipeline)
    manager._model_manager.get_model_info = lambda model_id: SimpleNamespace(
        model_id=model_id
    )

    threads = [threading.Thread(target=run_request) for _ in range(6)]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
    finally:
        manager.stop()

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    assert max_active_loads == 1
