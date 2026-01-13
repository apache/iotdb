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
import concurrent
import queue
import random
import threading
from concurrent.futures import wait
from typing import Dict, Optional

import torch
import torch.multiprocessing as mp

from iotdb.ainode.core.exception import InferenceModelInternalException
from iotdb.ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from iotdb.ainode.core.inference.inference_request_pool import (
    InferenceRequestPool,
    PoolState,
)
from iotdb.ainode.core.inference.pool_group import PoolGroup
from iotdb.ainode.core.inference.pool_scheduler.basic_pool_scheduler import (
    BasicPoolScheduler,
    ScaleActionType,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.util.atmoic_int import AtomicInt
from iotdb.ainode.core.util.batch_executor import BatchExecutor
from iotdb.ainode.core.util.decorator import synchronized
from iotdb.ainode.core.util.thread_name import ThreadName

logger = Logger()


class PoolController:
    """
    A controller for handling inference request pools.
    """

    def __init__(self, result_queue: mp.Queue):
        self._model_manager = ModelManager()
        # structure: {model_id: {device_id: PoolGroup}}
        self._request_pool_map: Dict[str, Dict[torch.device, PoolGroup]] = {}
        self._new_pool_id = AtomicInt()
        self._result_queue = result_queue
        self._pool_scheduler = BasicPoolScheduler(self._request_pool_map)
        self._stop_event = threading.Event()

        # for pool instances control
        self._task_queue = queue.Queue()
        self._pool_control_worker_thread = threading.Thread(
            target=self._worker_loop, daemon=True
        )
        self._pool_control_worker_thread.start()
        self._executor = BatchExecutor(
            thread_name_prefix=ThreadName.INFERENCE_POOL_CONTROLLER.value
        )

    # =============== Automatic Pool Management (Developing) ===============
    @synchronized(threading.Lock())
    def first_req_init(self, model_id: str, device):
        """
        Initialize the pools when the first request for the given model_id arrives.
        """
        pass
        # if not self.has_request_pools(model_id, device.index):
        #     # TODO: choose a device based on some strategy
        #     device = self.DEFAULT_DEVICE
        #     actions = self._pool_scheduler.schedule(model_id, device)
        #     for action in actions:
        #         if action.action == ScaleActionType.SCALE_UP:
        #             # initialize the first pool
        #             self._first_pool_init(action.model_id, str(device))
        #             # start a background thread to expand pools
        #             expand_thread = threading.Thread(
        #                 target=self._expand_pools_on_device,
        #                 args=(action.model_id, str(device), action.amount - 1),
        #                 daemon=True,
        #             )
        #             expand_thread.start()
        #         elif action.action == ScaleActionType.SCALE_DOWN:
        #             # TODO: implement scale down logic
        #             pass

    def _first_pool_init(self, model_id: str, device_str: str):
        """
        Initialize the first pool for the given model_id.
        Ensure the pool is ready before returning.
        """
        pass
        # device = torch.device(device_str)
        # device_id = device.index
        #
        # first_queue = mp.Queue()
        # ready_event = mp.Event()
        # first_pool = InferenceRequestPool(
        #     pool_id=0,
        #     model_id=model_id,
        #     device=device_str,
        #     request_queue=first_queue,
        #     result_queue=self._result_queue,
        #     ready_event=ready_event,
        # )
        # first_pool.start()
        # self._register_pool(model_id, device_str, 0, first_pool, first_queue)
        #
        # if not ready_event.wait(timeout=30):
        #     self._erase_pool(model_id, device_id, 0)
        #     logger.error(
        #         f"[Inference][{device}][Pool-0] Pool failed to be ready in time"
        #     )
        # else:
        #     self.set_state(model_id, device_id, 0, PoolState.RUNNING)
        #     logger.info(
        #         f"[Inference][{device}][Pool-0] Pool started running for model {model_id}"
        #     )

    # =============== Pool Management ===============
    def load_model(self, model_id: str, device_id_list: list[torch.device]):
        """
        Load the model to the specified devices asynchronously.
        Args:
            model_id (str): The ID of the model to be loaded.
            device_id_list (list[torch.device]): List of device_ids where the model should be loaded.
        """
        self._task_queue.put((self._load_model_task, (model_id, device_id_list), {}))

    def unload_model(self, model_id: str, device_id_list: list[torch.device]):
        """
        Unload the model from the specified devices asynchronously.
        Args:
            model_id (str): The ID of the model to be unloaded.
            device_id_list (list[torch.device]): List of device_ids where the model should be unloaded.
        """
        self._task_queue.put((self._unload_model_task, (model_id, device_id_list), {}))

    def show_loaded_models(
        self, device_id_list: list[torch.device]
    ) -> Dict[str, Dict[str, int]]:
        """
        Show loaded model instances on the specified devices.
        Args:
            device_id_list (list[torch.device]): List of device_ids where to examine loaded instances.
        Return:
            Dict[str, Dict[str, int]]: Dict[device_id, Dict[model_id, Count(instances)]].
        """
        result = {}
        for device_id in device_id_list:
            device_models = {}
            for model_id, device_map in self._request_pool_map.items():
                if device_id in device_map:
                    pool_group = device_map[device_id]
                    device_models[model_id] = pool_group.get_running_pool_count()
            device_key = (
                device_id.type if device_id.index is None else str(device_id.index)
            )
            result[device_key] = device_models
        return result

    def _worker_loop(self):
        while not self._stop_event.is_set():
            task = self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            task_fn, args, kwargs = task
            try:
                task_fn(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error executing task: {e}")
            finally:
                self._task_queue.task_done()

    def _load_model_task(self, model_id: str, device_id_list: list[torch.device]):
        def _load_model_on_device_task(device_id: torch.device):
            if not self.has_request_pools(model_id, device_id):
                actions = self._pool_scheduler.schedule_load_model_to_device(
                    self._model_manager.get_model_info(model_id), device_id
                )
                for action in actions:
                    if action.action == ScaleActionType.SCALE_UP:
                        self._expand_pools_on_device(
                            action.model_id, device_id, action.amount
                        )
                    elif action.action == ScaleActionType.SCALE_DOWN:
                        self._shrink_pools_on_device(
                            action.model_id, device_id, action.amount
                        )
            else:
                logger.info(
                    f"[Inference][{device_id}] Model {model_id} is already installed."
                )

        load_model_futures = self._executor.submit_batch(
            device_id_list, _load_model_on_device_task
        )
        concurrent.futures.wait(
            load_model_futures, return_when=concurrent.futures.ALL_COMPLETED
        )

    def _unload_model_task(self, model_id: str, device_id_list: list[torch.device]):
        def _unload_model_on_device_task(device_id: torch.device):
            if self.has_request_pools(model_id, device_id):
                actions = self._pool_scheduler.schedule_unload_model_from_device(
                    self._model_manager.get_model_info(model_id), device_id
                )
                for action in actions:
                    if action.action == ScaleActionType.SCALE_DOWN:
                        self._shrink_pools_on_device(
                            action.model_id, device_id, action.amount
                        )
                    elif action.action == ScaleActionType.SCALE_UP:
                        self._expand_pools_on_device(
                            action.model_id, device_id, action.amount
                        )
            else:
                logger.info(
                    f"[Inference][{device_id}] Model {model_id} is not installed."
                )

        unload_model_futures = self._executor.submit_batch(
            device_id_list, _unload_model_on_device_task
        )
        concurrent.futures.wait(
            unload_model_futures, return_when=concurrent.futures.ALL_COMPLETED
        )

    def _expand_pools_on_device(
        self, model_id: str, device_id: torch.device, count: int
    ):
        """
        Expand the pools for the given model_id and device_id sequentially.
        Args:
            model_id (str): The ID of the model.
            device_id (torch.device): The ID of the device.
            count (int): The number of pools to be expanded.
        """

        def _expand_pool_on_device(*_):
            request_queue = mp.Queue()
            pool_id = self._new_pool_id.get_and_increment()
            model_info = self._model_manager.get_model_info(model_id)
            pool = InferenceRequestPool(
                pool_id=pool_id,
                model_info=model_info,
                device=device_id,
                request_queue=request_queue,
                result_queue=self._result_queue,
                ready_event=mp.Event(),
            )
            pool.start()
            self._register_pool(model_id, device_id, pool_id, pool, request_queue)
            if not pool.ready_event.wait(timeout=300):
                logger.error(
                    f"[Inference][{device_id}][Pool-{pool_id}] Pool failed to be ready in time"
                )
                # TODO: retry or decrease the count? this error should be better handled
                self._erase_pool(model_id, device_id, pool_id)
            else:
                self.set_state(model_id, device_id, pool_id, PoolState.RUNNING)
                logger.info(
                    f"[Inference][{device_id}][Pool-{pool_id}] Pool started running for model {model_id}"
                )

        expand_pool_futures = self._executor.submit_batch(
            [None for _ in range(count)], _expand_pool_on_device
        )
        concurrent.futures.wait(
            expand_pool_futures, return_when=concurrent.futures.ALL_COMPLETED
        )

    def _shrink_pools_on_device(
        self, model_id: str, device_id: torch.device, count: int
    ):
        """
        Shrink the pools for the given model_id by count sequentially.
        TODO: shrink pools in parallel
        """

        pool_ids = self.get_pool_ids(model_id, device_id)
        current_count = len(pool_ids)
        if current_count < count:
            logger.warning(
                f"[Inference] Cannot shrink {count} pools for model {model_id}, only {current_count} pools available."
            )

        # 1. Find pools with zero load and set them to STOP
        zero_load_pools = [
            pid for pid in pool_ids if self.get_load(model_id, device_id, pid) == 0
        ]
        stop_count = min(len(zero_load_pools), count)
        pools_to_stop = zero_load_pools[:stop_count]

        for pid in pools_to_stop:
            self.set_state(
                model_id, device_id, pid, PoolState.STOPPING
            )  # Set to STOPPING state

        remaining_to_close = count - len(pools_to_stop)
        if remaining_to_close <= 0:
            # close the STOP pools whose load is 0
            for pid in pools_to_stop:
                self.get_request_pool(model_id, device_id, pid).stop()
                self._erase_pool(model_id, device_id, pid)
            return

        # 2. If not enough, randomly select additional pools to set to STOP
        candidates = [pid for pid in pool_ids if pid not in pools_to_stop]
        additional_stop = random.sample(
            candidates, min(remaining_to_close, len(candidates))
        )  # TODO: implement better selection strategy
        for pid in additional_stop:
            self.set_state(
                model_id, device_id, pid, PoolState.STOPPING
            )  # Set to STOPPING state

        # 3. Wait for all STOPPING pools to have zero load, then close them
        final_stop_pools = pools_to_stop + additional_stop
        while final_stop_pools and not self._stop_event.is_set():
            for pid in final_stop_pools[:]:  # iterate over a copy of the list
                if self.get_load(model_id, device_id, pid) == 0:
                    self.get_request_pool(model_id, device_id, pid).stop()
                    self._erase_pool(model_id, device_id, pid)
                    final_stop_pools.remove(pid)

    def _register_pool(
        self,
        model_id: str,
        device_id: torch.device,
        pool_id: int,
        request_pool: InferenceRequestPool,
        request_queue: mp.Queue,
    ):
        """
        Register a new inference request pool for the given model_id, device and pool_id.
        """
        self.set_request_pool_map(
            model_id, device_id, pool_id, request_pool, request_queue
        )
        pool_group: PoolGroup = self.get_request_pools_group(model_id, device_id)
        pool_group.set_state(pool_id, PoolState.INITIALIZING)
        logger.info(
            f"[Inference][{device_id}][Pool-{pool_id}] Pool initializing for model {model_id}"
        )

    def _erase_pool(self, model_id: str, device_id: torch.device, pool_id: int):
        """
        Erase the specified inference request pool for the given model_id, device_id and pool_id.
        """
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            pool_group.remove_pool(pool_id)
            logger.info(
                f"[Inference][{device_id}][Pool-{pool_id}] Erase pool for model {model_id}"
            )
        # Clean up empty structures
        if pool_group and not pool_group.get_pool_ids():
            self._request_pool_map[model_id].pop(device_id, None)
        if model_id in self._request_pool_map and not self._request_pool_map[model_id]:
            self._request_pool_map.pop(model_id, None)

    # =============== Request Management ===============
    def add_request(self, req: InferenceRequest, infer_proxy: InferenceRequestProxy):
        model_id = req.model_id
        if not self.has_request_pools(model_id):
            logger.error(f"[Inference] No pools found for model {model_id}.")
            infer_proxy.set_result(None)
            raise InferenceModelInternalException(
                "Dispatch request failed, because no inference pools are init."
            )
            # TODO: Implement adaptive scaling based on requests.(e.g. lazy initialization)
            # self.first_req_init(model_id)

        device_ids = self.get_device_ids(model_id)
        device_id = random.choice(
            device_ids
        )  # TODO: Implement better device selection strategy
        self._request_pool_map[model_id][device_id].dispatch_request(req, infer_proxy)

    # =============== Getters / Setters ===============
    def get_state(
        self, model_id: str, device_id: torch.device, pool_id: int
    ) -> Optional[PoolState]:
        """
        Get the state of the specified pool based on model_id, device_id, and pool_id.
        """
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            return pool_group.get_state(pool_id)
        return None

    def set_state(
        self, model_id: str, device_id: torch.device, pool_id: int, state: PoolState
    ):
        """
        Set the state of the specified pool based on model_id, device_id, and pool_id.
        """
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            pool_group.set_state(pool_id, state)

    def get_device_ids(self, model_id) -> list[torch.device]:
        """
        Get the list of device IDs for the given model_id, where the corresponding instances are loaded.
        """
        if model_id in self._request_pool_map:
            return list(self._request_pool_map[model_id].keys())
        return []

    def get_pool_ids(self, model_id: str, device_id: torch.device) -> list[int]:
        """
        Get the list of pool IDs for the given model_id and device_id.
        """
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            return pool_group.get_pool_ids()
        return []

    def has_request_pools(self, model_id: str, device_id: torch.device = None) -> bool:
        """
        Check if there are request pools for the given model_id ((optional) and device_id).
        """
        if model_id not in self._request_pool_map:
            return False
        if device_id is not None:
            return device_id in self._request_pool_map[model_id]
        return True

    def get_request_pools_group(
        self, model_id: str, device_id: torch.device
    ) -> Optional[PoolGroup]:
        if (
            model_id in self._request_pool_map
            and device_id in self._request_pool_map[model_id]
        ):
            return self._request_pool_map[model_id][device_id]
        else:
            return None

    def get_request_pool(
        self, model_id: str, device_id: torch.device, pool_id: int
    ) -> Optional[InferenceRequestPool]:
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            return pool_group.get_request_pool(pool_id)
        return None

    def get_request_queue(
        self, model_id: str, device_id: torch.device, pool_id: int
    ) -> Optional[mp.Queue]:
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            return pool_group.get_request_queue(pool_id)
        return None

    def set_request_pool_map(
        self,
        model_id: str,
        device_id: torch.device,
        pool_id: int,
        request_pool: InferenceRequestPool,
        request_queue: mp.Queue,
    ):
        """
        Set the request pool map for the given model_id, device and pool_id.
        """
        if model_id not in self._request_pool_map:
            self._request_pool_map[model_id] = {}
        if device_id not in self._request_pool_map[model_id]:
            self._request_pool_map[model_id][device_id] = PoolGroup(model_id)
        self._request_pool_map[model_id][device_id].add_pool(
            pool_id, request_pool, request_queue
        )
        logger.info(
            f"[Inference][{device_id}][Pool-{pool_id}] Registered pool for model {model_id}"
        )

    def get_load(self, model_id: str, device_id: torch.device, pool_id: int) -> int:
        """
        Get the current load of the specified pool.
        """
        pool_group = self.get_request_pools_group(model_id, device_id)
        if pool_group:
            return pool_group.get_load(pool_id)
        return -1

    def stop(self):
        self._stop_event.set()

        # shutdown pool controller
        self._task_queue.put(None)
        self._pool_control_worker_thread.join()
        self._executor.close()

        # shutdown pool instances
        # TODO: pool instances can be shutdown in parallel
        for inner in self._request_pool_map.values():
            for group in inner.values():
                group.shutdown()
