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
1import concurrent
1import queue
1import random
1import threading
1from concurrent.futures import wait
1from typing import Dict, Optional
1
1import torch
1import torch.multiprocessing as mp
1
1from iotdb.ainode.core.exception import InferenceModelInternalError
1from iotdb.ainode.core.inference.inference_request import (
1    InferenceRequest,
1    InferenceRequestProxy,
1)
1from iotdb.ainode.core.inference.inference_request_pool import (
1    InferenceRequestPool,
1    PoolState,
1)
1from iotdb.ainode.core.inference.pool_group import PoolGroup
1from iotdb.ainode.core.inference.pool_scheduler.basic_pool_scheduler import (
1    BasicPoolScheduler,
1    ScaleActionType,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.model.sundial.configuration_sundial import SundialConfig
1from iotdb.ainode.core.model.timerxl.configuration_timer import TimerConfig
1from iotdb.ainode.core.util.atmoic_int import AtomicInt
1from iotdb.ainode.core.util.batch_executor import BatchExecutor
1from iotdb.ainode.core.util.decorator import synchronized
1from iotdb.ainode.core.util.thread_name import ThreadName
1
1logger = Logger()
1
1
1class PoolController:
1    """
1    A controller for handling inference request pools.
1    """
1
1    def __init__(self, result_queue: mp.Queue):
1        # structure: {model_id: {device_id: PoolGroup}}
1        self._request_pool_map: Dict[str, Dict[str, PoolGroup]] = {}
1        self._new_pool_id = AtomicInt()
1        self._result_queue = result_queue
1        self._pool_scheduler = BasicPoolScheduler(self._request_pool_map)
1        self._stop_event = threading.Event()
1
1        # for pool instances control
1        self._task_queue = queue.Queue()
1        self._pool_control_worker_thread = threading.Thread(
1            target=self._worker_loop, daemon=True
1        )
1        self._pool_control_worker_thread.start()
1        self._executor = BatchExecutor(
1            thread_name_prefix=ThreadName.INFERENCE_POOL_CONTROLLER.value
1        )
1
1    # =============== Pool Management ===============
1    @synchronized(threading.Lock())
1    def first_req_init(self, model_id: str):
1        """
1        Initialize the pools when the first request for the given model_id arrives.
1        """
1        if not self.has_request_pools(model_id, device.index):
1            # TODO: choose a device based on some strategy
1            device = self.DEFAULT_DEVICE
1            actions = self._pool_scheduler.schedule(model_id, device)
1            for action in actions:
1                if action.action == ScaleActionType.SCALE_UP:
1                    # initialize the first pool
1                    self._first_pool_init(action.model_id, str(device))
1                    # start a background thread to expand pools
1                    expand_thread = threading.Thread(
1                        target=self._expand_pools_on_device,
1                        args=(action.model_id, str(device), action.amount - 1),
1                        daemon=True,
1                    )
1                    expand_thread.start()
1                elif action.action == ScaleActionType.SCALE_DOWN:
1                    # TODO: implement scale down logic
1                    pass
1
1    def _first_pool_init(self, model_id: str, device_str: str):
1        """
1        Initialize the first pool for the given model_id.
1        Ensure the pool is ready before returning.
1        """
1        device = torch.device(device_str)
1        device_id = device.index
1
1        if model_id == "sundial":
1            config = SundialConfig()
1        elif model_id == "timer_xl":
1            config = TimerConfig()
1        first_queue = mp.Queue()
1        ready_event = mp.Event()
1        first_pool = InferenceRequestPool(
1            pool_id=0,
1            model_id=model_id,
1            device=device_str,
1            config=config,
1            request_queue=first_queue,
1            result_queue=self._result_queue,
1            ready_event=ready_event,
1        )
1        first_pool.start()
1        self._register_pool(model_id, device_str, 0, first_pool, first_queue)
1
1        if not ready_event.wait(timeout=30):
1            self._erase_pool(model_id, device_id, 0)
1            logger.error(
1                f"[Inference][Device-{device}][Pool-0] Pool failed to be ready in time"
1            )
1        else:
1            self.set_state(model_id, device_id, 0, PoolState.RUNNING)
1            logger.info(
1                f"[Inference][Device-{device}][Pool-0] Pool started running for model {model_id}"
1            )
1
1    def load_model(self, model_id: str, device_id_list: list[str]):
1        """
1        Load the model to the specified devices asynchronously.
1        Args:
1            model_id (str): The ID of the model to be loaded.
1            device_id_list (list[str]): List of device_ids where the model should be loaded.
1        """
1        self._task_queue.put((self._load_model_task, (model_id, device_id_list), {}))
1
1    def unload_model(self, model_id: str, device_id_list: list[str]):
1        """
1        Unload the model from the specified devices asynchronously.
1        Args:
1            model_id (str): The ID of the model to be unloaded.
1            device_id_list (list[str]): List of device_ids where the model should be unloaded.
1        """
1        self._task_queue.put((self._unload_model_task, (model_id, device_id_list), {}))
1
1    def show_loaded_models(
1        self, device_id_list: list[str]
1    ) -> Dict[str, Dict[str, int]]:
1        """
1        Show loaded model instances on the specified devices.
1        Args:
1            device_id_list (list[str]): List of device_ids where to examine loaded instances.
1        Return:
1            Dict[str, Dict[str, int]]: Dict[device_id, Dict[model_id, Count(instances)]].
1        """
1        result = {}
1        for device_id in device_id_list:
1            device_models = {}
1            for model_id, device_map in self._request_pool_map.items():
1                if device_id in device_map:
1                    pool_group = device_map[device_id]
1                    device_models[model_id] = pool_group.get_pool_count()
1            result[device_id] = device_models
1        return result
1
1    def _worker_loop(self):
1        while not self._stop_event.is_set():
1            task = self._task_queue.get()
1            if task is None:
1                self._task_queue.task_done()
1                break
1            task_fn, args, kwargs = task
1            try:
1                task_fn(*args, **kwargs)
1            except Exception as e:
1                logger.error(f"Error executing task: {e}")
1            finally:
1                self._task_queue.task_done()
1
1    def _load_model_task(self, model_id: str, device_id_list: list[str]):
1        def _load_model_on_device_task(device_id: str):
1            if not self.has_request_pools(model_id, device_id):
1                actions = self._pool_scheduler.schedule_load_model_to_device(
1                    model_id, device_id
1                )
1                for action in actions:
1                    if action.action == ScaleActionType.SCALE_UP:
1                        self._expand_pools_on_device(
1                            action.model_id, device_id, action.amount
1                        )
1                    elif action.action == ScaleActionType.SCALE_DOWN:
1                        self._shrink_pools_on_device(
1                            action.model_id, device_id, action.amount
1                        )
1            else:
1                logger.info(
1                    f"[Inference][Device-{device_id}] Model {model_id} is already installed."
1                )
1
1        load_model_futures = self._executor.submit_batch(
1            device_id_list, _load_model_on_device_task
1        )
1        concurrent.futures.wait(
1            load_model_futures, return_when=concurrent.futures.ALL_COMPLETED
1        )
1
1    def _unload_model_task(self, model_id: str, device_id_list: list[str]):
1        def _unload_model_on_device_task(device_id: str):
1            if self.has_request_pools(model_id, device_id):
1                actions = self._pool_scheduler.schedule_unload_model_from_device(
1                    model_id, device_id
1                )
1                for action in actions:
1                    if action.action == ScaleActionType.SCALE_DOWN:
1                        self._shrink_pools_on_device(
1                            action.model_id, device_id, action.amount
1                        )
1                    elif action.action == ScaleActionType.SCALE_UP:
1                        self._expand_pools_on_device(
1                            action.model_id, device_id, action.amount
1                        )
1            else:
1                logger.info(
1                    f"[Inference][Device-{device_id}] Model {model_id} is not installed."
1                )
1
1        unload_model_futures = self._executor.submit_batch(
1            device_id_list, _unload_model_on_device_task
1        )
1        concurrent.futures.wait(
1            unload_model_futures, return_when=concurrent.futures.ALL_COMPLETED
1        )
1
1    def _expand_pools_on_device(self, model_id: str, device_id: str, count: int):
1        """
1        Expand the pools for the given model_id and device_id sequentially.
1        Args:
1            model_id (str): The ID of the model.
1            device_id (str): The ID of the device.
1            count (int): The number of pools to be expanded.
1        """
1
1        def _expand_pool_on_device(*_):
1            result_queue = mp.Queue()
1            pool_id = self._new_pool_id.get_and_increment()
1            if model_id == "sundial":
1                config = SundialConfig()
1            elif model_id == "timer_xl":
1                config = TimerConfig()
1            pool = InferenceRequestPool(
1                pool_id=pool_id,
1                model_id=model_id,
1                device=device_id,
1                config=config,
1                request_queue=result_queue,
1                result_queue=self._result_queue,
1                ready_event=mp.Event(),
1            )
1            pool.start()
1            self._register_pool(model_id, device_id, pool_id, pool, result_queue)
1            if not pool.ready_event.wait(timeout=30):
1                logger.error(
1                    f"[Inference][Device-{device_id}][Pool-{pool_id}] Pool failed to be ready in time"
1                )
1                # TODO: retry or decrease the count? this error should be better handled
1                self._erase_pool(model_id, device_id, pool_id)
1            else:
1                self.set_state(model_id, device_id, pool_id, PoolState.RUNNING)
1                logger.info(
1                    f"[Inference][Device-{device_id}][Pool-{pool_id}] Pool started running for model {model_id}"
1                )
1
1        expand_pool_futures = self._executor.submit_batch(
1            [None for _ in range(count)], _expand_pool_on_device
1        )
1        concurrent.futures.wait(
1            expand_pool_futures, return_when=concurrent.futures.ALL_COMPLETED
1        )
1
1    def _shrink_pools_on_device(self, model_id: str, device_id: str, count):
1        """
1        Shrink the pools for the given model_id by count sequentially.
1        TODO: shrink pools in parallel
1        """
1
1        pool_ids = self.get_pool_ids(model_id, device_id)
1        current_count = len(pool_ids)
1        if current_count < count:
1            logger.warning(
1                f"[Inference] Cannot shrink {count} pools for model {model_id}, only {current_count} pools available."
1            )
1
1        # 1. Find pools with zero load and set them to STOP
1        zero_load_pools = [
1            pid for pid in pool_ids if self.get_load(model_id, device_id, pid) == 0
1        ]
1        stop_count = min(len(zero_load_pools), count)
1        pools_to_stop = zero_load_pools[:stop_count]
1
1        for pid in pools_to_stop:
1            self.set_state(
1                model_id, device_id, pid, PoolState.STOPPING
1            )  # Set to STOPPING state
1
1        remaining_to_close = count - len(pools_to_stop)
1        if remaining_to_close <= 0:
1            # close the STOP pools whose load is 0
1            for pid in pools_to_stop:
1                self.get_request_pool(model_id, device_id, pid).stop()
1                self._erase_pool(model_id, device_id, pid)
1            return
1
1        # 2. If not enough, randomly select additional pools to set to STOP
1        candidates = [pid for pid in pool_ids if pid not in pools_to_stop]
1        additional_stop = random.sample(
1            candidates, min(remaining_to_close, len(candidates))
1        )  # TODO: implement better selection strategy
1        for pid in additional_stop:
1            self.set_state(
1                model_id, device_id, pid, PoolState.STOPPING
1            )  # Set to STOPPING state
1
1        # 3. Wait for all STOPPING pools to have zero load, then close them
1        final_stop_pools = pools_to_stop + additional_stop
1        while final_stop_pools and not self._stop_event.is_set():
1            for pid in final_stop_pools[:]:  # iterate over a copy of the list
1                if self.get_load(model_id, device_id, pid) == 0:
1                    self.get_request_pool(model_id, device_id, pid).stop()
1                    self._erase_pool(model_id, device_id, pid)
1                    final_stop_pools.remove(pid)
1
1    def _register_pool(
1        self,
1        model_id: str,
1        device_id: str,
1        pool_id: int,
1        request_pool: InferenceRequestPool,
1        request_queue: mp.Queue,
1    ):
1        """
1        Register a new inference request pool for the given model_id, device and pool_id.
1        """
1        self.set_request_pool_map(
1            model_id, device_id, pool_id, request_pool, request_queue
1        )
1        pool_group: PoolGroup = self.get_request_pools_group(model_id, device_id)
1        pool_group.set_state(pool_id, PoolState.INITIALIZING)
1        logger.info(
1            f"[Inference][Device-{device_id}][Pool-{pool_id}] Pool initializing for model {model_id}"
1        )
1
1    def _erase_pool(self, model_id: str, device_id: str, pool_id: int):
1        """
1        Erase the specified inference request pool for the given model_id, device_id and pool_id.
1        """
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            pool_group.remove_pool(pool_id)
1            logger.info(
1                f"[Inference][Device-{device_id}][Pool-{pool_id}] Erase pool for model {model_id}"
1            )
1        # Clean up empty structures
1        if pool_group and not pool_group.get_pool_ids():
1            self._request_pool_map[model_id].pop(device_id, None)
1        if model_id in self._request_pool_map and not self._request_pool_map[model_id]:
1            self._request_pool_map.pop(model_id, None)
1
1    # =============== Request Management ===============
1    def add_request(self, req: InferenceRequest, infer_proxy: InferenceRequestProxy):
1        model_id = req.model_id
1        if not self.has_request_pools(model_id):
1            logger.error(f"[Inference] No pools found for model {model_id}.")
1            infer_proxy.set_result(None)
1            raise InferenceModelInternalError(
1                "Dispatch request failed, because no inference pools are init."
1            )
1            # TODO: Implement adaptive scaling based on requests.(e.g. lazy initialization)
1            # self.first_req_init(model_id)
1
1        device_ids = self.get_device_ids(model_id)
1        device_id = random.choice(
1            device_ids
1        )  # TODO: Implement better device selection strategy
1        self._request_pool_map[model_id][device_id].dispatch_request(req, infer_proxy)
1
1    # =============== Getters / Setters ===============
1    def get_state(self, model_id, device_id, pool_id) -> Optional[PoolState]:
1        """
1        Get the state of the specified pool based on model_id, device_id, and pool_id.
1        """
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            return pool_group.get_state(pool_id)
1        return None
1
1    def set_state(self, model_id, device_id, pool_id, state):
1        """
1        Set the state of the specified pool based on model_id, device_id, and pool_id.
1        """
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            pool_group.set_state(pool_id, state)
1
1    def get_device_ids(self, model_id) -> list[str]:
1        """
1        Get the list of device IDs for the given model_id, where the corresponding instances are loaded.
1        """
1        if model_id in self._request_pool_map:
1            return list(self._request_pool_map[model_id].keys())
1        return []
1
1    def get_pool_ids(self, model_id: str, device_id: str) -> list[int]:
1        """
1        Get the list of pool IDs for the given model_id and device_id.
1        """
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            return pool_group.get_pool_ids()
1        return []
1
1    def has_request_pools(self, model_id: str, device_id: Optional[str] = None) -> bool:
1        """
1        Check if there are request pools for the given model_id and device_id (optional).
1        """
1        if model_id not in self._request_pool_map:
1            return False
1        if device_id is not None:
1            return device_id in self._request_pool_map[model_id]
1        return True
1
1    def get_request_pools_group(
1        self, model_id: str, device_id: str
1    ) -> Optional[PoolGroup]:
1        if (
1            model_id in self._request_pool_map
1            and device_id in self._request_pool_map[model_id]
1        ):
1            return self._request_pool_map[model_id][device_id]
1        else:
1            return None
1
1    def get_request_pool(
1        self, model_id, device_id, pool_id
1    ) -> Optional[InferenceRequestPool]:
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            return pool_group.get_request_pool(pool_id)
1        return None
1
1    def get_request_queue(self, model_id, device_id, pool_id) -> Optional[mp.Queue]:
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            return pool_group.get_request_queue(pool_id)
1        return None
1
1    def set_request_pool_map(
1        self,
1        model_id: str,
1        device_id: str,
1        pool_id: int,
1        request_pool: InferenceRequestPool,
1        request_queue: mp.Queue,
1    ):
1        """
1        Set the request pool map for the given model_id, device and pool_id.
1        """
1        if model_id not in self._request_pool_map:
1            self._request_pool_map[model_id] = {}
1        if device_id not in self._request_pool_map[model_id]:
1            self._request_pool_map[model_id][device_id] = PoolGroup(model_id)
1        self._request_pool_map[model_id][device_id].add_pool(
1            pool_id, request_pool, request_queue
1        )
1        logger.info(
1            f"[Inference][Device-{device_id}][Pool-{pool_id}] Registered pool for model {model_id}"
1        )
1
1    def get_load(self, model_id, device_id, pool_id) -> int:
1        """
1        Get the current load of the specified pool.
1        """
1        pool_group = self.get_request_pools_group(model_id, device_id)
1        if pool_group:
1            return pool_group.get_load(pool_id)
1        return -1
1
1    def shutdown(self):
1        self._stop_event.set()
1
1        # shutdown pool controller
1        self._task_queue.put(None)
1        self._pool_control_worker_thread.join()
1        self._executor.close()
1
1        # shutdown pool instances
1        # TODO: pool instances can be shutdown in parallel
1        for inner in self._request_pool_map.values():
1            for group in inner.values():
1                group.shutdown()
1