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
from collections import Counter
from unittest import mock

import torch

from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.inference.pool_controller import PoolController
from iotdb.ainode.core.manager.inference_manager import InferenceManager
from iotdb.ainode.core.model.model_constants import ModelStates


class BuiltinModelLoadTest(unittest.TestCase):
    """LOAD MODEL downloads on-request weights without blocking the pool controller."""

    def setUp(self):
        self.release_download = threading.Event()
        self.release_unload = threading.Event()
        self.release_unload.set()
        self.download_succeeds = True
        self.events = []
        self.models = {
            "timestral": mock.Mock(
                download_on_start=False,
                state=ModelStates.INACTIVE,
                weights_license_notice="Check the weights licence.",
            ),
            "sundial": mock.Mock(
                download_on_start=True,
                state=ModelStates.ACTIVE,
                weights_license_notice=None,
            ),
        }
        self.controller = self._controller()

    def _controller(self, task_queue=None):
        controller = object.__new__(PoolController)
        controller._stop_event = threading.Event()
        controller._task_queue = task_queue if task_queue is not None else queue.Queue()
        controller._tasks_waiting_for_download = {}
        controller._pending_loads = Counter()
        controller._pending_unloads = Counter()
        controller._pending_lock = threading.Lock()
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

        def unload(model_id, device_id_list):
            self.release_unload.wait(10)
            self.events.append(("unload", model_id))
            controller._request_pool_map.pop(model_id, None)
            controller._finish_pending_unloads(model_id, device_id_list)

        controller._unload_one_model_task = unload
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
        self.release_unload.set()
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

    def test_unload_through_inference_manager_runs_after_a_pending_download(self):
        # UNLOAD MODEL while the model downloads must not be skipped because the model has no
        # pool yet: it runs after the load, and nothing is left pending.
        self.addCleanup(self._stop)
        manager = object.__new__(InferenceManager)
        manager._pool_controller = self.controller
        cpu = torch.device("cpu")
        self.assertEqual(
            manager.load_model("timestral", [cpu]).code,
            TSStatusCode.SUCCESS_STATUS.value,
        )
        self.assertTrue(self.controller.is_loading("timestral", cpu))
        self.assertEqual(
            manager.unload_model("timestral", [cpu]).code,
            TSStatusCode.SUCCESS_STATUS.value,
        )
        self.release_download.set()
        self._wait_for(2)
        self.assertEqual(self.events, [("load", "timestral"), ("unload", "timestral")])
        self.assertFalse(self.controller.is_loading("timestral", cpu))

    def test_unload_racing_a_load_through_inference_manager_runs_after_it(self):
        # LOAD MODEL publishes the load as pending and queues it. An UNLOAD MODEL arriving between
        # the two must not be queued first, or it finds no pool and the load then leaves the
        # model loaded. The queue holds the load's put() open while the unload is issued.
        self.controller.stop()
        entered, release = threading.Event(), threading.Event()
        controller = None

        class GatedQueue(queue.Queue):
            def put(self, item, *args, **kwargs):
                if item is not None and item[0] == controller._load_one_model_task:
                    entered.set()
                    release.wait(10)
                super().put(item, *args, **kwargs)

        controller = self.controller = self._controller(GatedQueue())
        self.addCleanup(self._stop)
        manager = object.__new__(InferenceManager)
        manager._pool_controller = controller
        cpu = torch.device("cpu")

        loading = threading.Thread(target=manager.load_model, args=("sundial", [cpu]))
        loading.start()
        self.assertTrue(entered.wait(5))
        unloaded = threading.Event()

        def unload():
            manager.unload_model("sundial", [cpu])
            unloaded.set()

        unloading = threading.Thread(target=unload)
        unloading.start()
        # The unload must wait for the load to be queued; give it the chance to overtake.
        self.assertFalse(unloaded.wait(0.5))
        release.set()
        loading.join(5)
        unloading.join(5)
        self._wait_for(2)
        self.assertEqual(self.events, [("load", "sundial"), ("unload", "sundial")])

    def test_load_after_a_pending_unload_through_inference_manager_runs_after_it(self):
        # UNLOAD MODEL is queued while the model still has its pool; a LOAD MODEL that arrives
        # before the unload has run must not be skipped as already loaded, or the unload then
        # removes the pool and the model ends up unloaded although the load came last.
        self.addCleanup(self._stop)
        manager = object.__new__(InferenceManager)
        manager._pool_controller = self.controller
        cpu = torch.device("cpu")
        self.controller._request_pool_map["sundial"] = {cpu: mock.Mock()}
        self.release_unload.clear()

        manager.unload_model("sundial", [cpu])
        self.assertTrue(self.controller.is_unloading("sundial", cpu))
        manager.load_model("sundial", [cpu])
        self.assertTrue(self.controller.is_loading("sundial", cpu))

        self.release_unload.set()
        self._wait_for(2)
        self.assertEqual(self.events, [("unload", "sundial"), ("load", "sundial")])
        self.assertFalse(self.controller.is_unloading("sundial", cpu))

    def _pause_check_of(self, thread_name):
        """Hold the named thread inside its pool check until released, after signalling it."""
        entered, release = threading.Event(), threading.Event()
        has_request_pools = self.controller.has_request_pools

        def check(model_id, device_id=None):
            if threading.current_thread().name == thread_name and not entered.is_set():
                entered.set()
                release.wait(10)
            return has_request_pools(model_id, device_id)

        self.controller.has_request_pools = check
        return entered, release

    def _race(self, first, second):
        """Run first until it is inside its pool check, then second; return whether second
        finished while first was held there."""
        entered, release = self._pause_check_of("first")
        threads = [
            threading.Thread(target=first, name="first"),
            threading.Thread(target=second, name="second"),
        ]
        threads[0].start()
        self.assertTrue(entered.wait(5))
        threads[1].start()
        threads[1].join(0.5)
        overtook = not threads[1].is_alive()
        release.set()
        for thread in threads:
            thread.join(5)
        return overtook

    def test_unload_during_a_load_check_is_ordered_after_the_load(self):
        # LOAD MODEL finds the model loaded and skips; an UNLOAD MODEL issued while the LOAD is
        # still deciding must wait for it, and then unloads: the outcome of LOAD, then UNLOAD.
        self.addCleanup(self._stop)
        manager = object.__new__(InferenceManager)
        manager._pool_controller = self.controller
        cpu = torch.device("cpu")
        self.controller._request_pool_map["sundial"] = {cpu: mock.Mock()}

        overtook = self._race(
            lambda: manager.load_model("sundial", [cpu]),
            lambda: manager.unload_model("sundial", [cpu]),
        )
        self.assertFalse(overtook)
        self._wait_for(1)
        self.assertEqual(self.events, [("unload", "sundial")])
        self.assertFalse(self.controller.is_loading("sundial", cpu))

    def test_load_during_an_unload_check_is_ordered_after_the_unload(self):
        # UNLOAD MODEL finds the model loaded; a LOAD MODEL issued while the UNLOAD is still
        # deciding must not skip on the pool the UNLOAD is about to remove: it waits, sees the
        # unload pending and loads again, the outcome of UNLOAD, then LOAD.
        self.addCleanup(self._stop)
        manager = object.__new__(InferenceManager)
        manager._pool_controller = self.controller
        cpu = torch.device("cpu")
        self.controller._request_pool_map["sundial"] = {cpu: mock.Mock()}

        overtook = self._race(
            lambda: manager.unload_model("sundial", [cpu]),
            lambda: manager.load_model("sundial", [cpu]),
        )
        self.assertFalse(overtook)
        self._wait_for(2)
        self.assertEqual(self.events, [("unload", "sundial"), ("load", "sundial")])

    def test_failed_download_finishes_the_pending_load(self):
        self.addCleanup(self._stop)
        self.download_succeeds = False
        cpu = torch.device("cpu")
        self.controller.load_model("timestral", [cpu])
        self.release_download.set()
        deadline = time.monotonic() + 5
        while self.controller.is_loading("timestral", cpu) and (
            time.monotonic() < deadline
        ):
            time.sleep(0.01)
        self.assertFalse(self.controller.is_loading("timestral", cpu))

    def test_load_logs_the_weights_license_notice(self):
        self.addCleanup(self._stop)
        with mock.patch("iotdb.ainode.core.inference.pool_controller.logger") as logger:
            self.controller.load_model("timestral", [torch.device("cpu")])
            self.controller.load_model("sundial", [torch.device("cpu")])
            self._wait_for(1)
        warnings = [call.args[0] for call in logger.warning.call_args_list]
        self.assertEqual(len(warnings), 1)
        self.assertIn("timestral", warnings[0])
        self.assertIn("Check the weights licence.", warnings[0])

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
