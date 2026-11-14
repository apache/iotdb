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

import queue
import threading
import time
import unittest
from unittest import mock

import torch

from iotdb.ainode.core.inference.pool_controller import PoolController
from iotdb.ainode.core.model.model_constants import ModelStates


class BuiltinModelLoadTest(unittest.TestCase):
    """LOAD MODEL downloads on-request weights without blocking the pool controller."""

    def setUp(self):
        self.release_download = threading.Event()
        self.download_succeeds = True
        self.events = []
        self.models = {
            "timestral": mock.Mock(download_on_start=False, state=ModelStates.INACTIVE),
            "sundial": mock.Mock(download_on_start=True, state=ModelStates.ACTIVE),
        }
        self.controller = self._controller()

    def _controller(self):
        controller = object.__new__(PoolController)
        controller._stop_event = threading.Event()
        controller._task_queue = queue.Queue()
        controller._tasks_waiting_for_download = {}
        controller._executor = mock.Mock()
        controller._request_pool_map = {}
        controller._model_manager = mock.Mock()
        controller._model_manager.get_model_info.side_effect = self.models.get
        controller._model_manager.download_builtin_model.side_effect = self._download

        def load_on_devices(model_ids, task):
            self.events.append(("load", self._model_being_loaded))
            return []

        def load(model_id, device_id_list):
            self._model_being_loaded = model_id
            PoolController._load_one_model_task(controller, model_id, device_id_list)

        controller._executor.submit_batch.side_effect = load_on_devices
        controller._load_one_model_task = load
        controller._unload_one_model_task = lambda model_id, _: self.events.append(
            ("unload", model_id)
        )
        controller._pool_control_worker_thread = threading.Thread(
            target=controller._worker_loop, daemon=True
        )
        controller._pool_control_worker_thread.start()
        return controller

    def _download(self, model_id):
        self.release_download.wait(10)
        if self.download_succeeds:
            self.models[model_id].state = ModelStates.ACTIVE
        return self.download_succeeds

    def _wait_for(self, count):
        deadline = time.monotonic() + 5
        while len(self.events) < count and time.monotonic() < deadline:
            time.sleep(0.01)

    def _stop(self):
        self.release_download.set()
        self.controller.stop()

    def test_other_models_are_not_blocked_by_a_download(self):
        self.addCleanup(self._stop)
        cpu = [torch.device("cpu")]
        self.controller.load_model("timestral", cpu)
        self.controller.unload_model("timestral", cpu)
        self.controller.load_model("sundial", cpu)
        self.controller.unload_model("sundial", cpu)
        self._wait_for(2)
        # sundial's tasks run while timestral is still downloading
        self.assertEqual(self.events, [("load", "sundial"), ("unload", "sundial")])

        self.release_download.set()
        self._wait_for(4)
        # timestral's unload runs after its load, in the order they were requested
        self.assertEqual(
            self.events[2:], [("load", "timestral"), ("unload", "timestral")]
        )

    def test_failed_download_skips_the_load_only(self):
        self.addCleanup(self._stop)
        self.download_succeeds = False
        cpu = [torch.device("cpu")]
        self.controller.load_model("timestral", cpu)
        self.controller.unload_model("timestral", cpu)
        self.release_download.set()
        self._wait_for(1)
        self.assertEqual(self.events, [("unload", "timestral")])

    def test_stop_does_not_wait_for_a_download(self):
        self.controller.load_model("timestral", [torch.device("cpu")])
        deadline = time.monotonic() + 5
        while (
            not self.controller._model_manager.download_builtin_model.called
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        started = time.monotonic()
        self.controller.stop()
        self.assertLess(time.monotonic() - started, 3)
        self.release_download.set()


if __name__ == "__main__":
    unittest.main()
