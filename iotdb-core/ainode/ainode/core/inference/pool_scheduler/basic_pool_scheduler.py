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

from typing import Dict, List

import torch

from ainode.core.exception import InferenceModelInternalError
from ainode.core.inference.pool_group import PoolGroup
from ainode.core.inference.pool_scheduler.abstract_pool_scheduler import (
    AbstractPoolScheduler,
    ScaleAction,
    ScaleActionType,
)
from ainode.core.log import Logger
from ainode.core.manager.utils import (
    estimate_pool_size,
    estimate_shared_pool_size_by_total_mem,
)

logger = Logger()


class BasicPoolScheduler(AbstractPoolScheduler):
    """
    A basic scheduler to init the request pools.
    """

    DEFAULT_DEVICE = torch.device("cpu")
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def __init__(self, request_pool_map: Dict[str, Dict[int, PoolGroup]]):
        super().__init__(request_pool_map)

    def schedule(self, model_id: str, device: torch.device) -> List[ScaleAction]:
        """
        Schedule a scaling action for the given model_id and device.
        """
        if model_id not in self._request_pool_map:
            pool_num = estimate_pool_size(device, model_id)
            if pool_num <= 0:
                raise InferenceModelInternalError(
                    f"Not enough memory to run model {model_id} on {device}."
                )
            return [
                ScaleAction(ScaleActionType.SCALE_UP, pool_num, model_id, device.index)
            ]
        return []

    def schedule_install(
        self, model_id: str, device: torch.device
    ) -> List[ScaleAction]:
        """
        Schedule scaling actions when installing a model on a given device.
        Strategy:
        - If device has no pools: estimate and allocate.
        - If other models exist on device:
            * Ensure all models (including new one) have at least one pool.
            * If not possible, raise error.
            * Else: scale up new model by 1, scale down others if needed,
                then allocate remaining pools to new model.
        """
        actions: List[ScaleAction] = []

        # The pool groups on the device
        device_pool_groups = {
            model_id: dev_map[device.index]
            for model_id, dev_map in self._request_pool_map.items()
            if device.index in dev_map
        }

        # case1: there is no pool on the device
        if not device_pool_groups:
            pool_num = estimate_pool_size(device, model_id)
            if pool_num <= 0:
                raise InferenceModelInternalError(
                    f"Not enough memory to run model {model_id} on {device}."
                )
            return [
                ScaleAction(ScaleActionType.SCALE_UP, pool_num, model_id, device.index)
            ]

        # case2: there are already some pools on the device
        pool_nums = estimate_shared_pool_size_by_total_mem(
            device, list(device_pool_groups.keys()), model_id
        )
        if not pool_nums or model_id not in pool_nums:
            logger.error(
                f"[Inference][Device-{device}] Not enough TOTAL memory to guarantee at least 1 pool for model {model_id}."
            )
            return []

        device_id = device.index

        # step1. allocate 1 pool for the new model
        actions.append(ScaleAction(ScaleActionType.SCALE_UP, 1, model_id, device_id))

        # step2. calculate how many pools to scale down for other models
        for mid, pg in device_pool_groups.items():
            current_num = len(pg.get_pool_ids())
            target_num = pool_nums[mid]
            diff = target_num - current_num
            if diff < 0:
                actions.append(
                    ScaleAction(ScaleActionType.SCALE_DOWN, -diff, mid, device_id)
                )

        # step3. allocate remaining pools to the new model
        target_new = pool_nums[model_id]
        remaining_up = target_new - 1
        if remaining_up > 0:
            actions.append(
                ScaleAction(ScaleActionType.SCALE_UP, remaining_up, model_id, device_id)
            )

        return actions

    def schedule_uninstall(
        self, model_id: str, device: torch.device
    ) -> List[ScaleAction]:
        """
        Schedule scaling actions when uninstalling a model from a given device.
        Strategy:
        - If the device only has this model: scale it down completely.
        - If multiple models exist:
            * Scale down all pools of the given model_id.
            * Redistribute total memory across remaining models.
            * If expansion is needed for other models, append SCALE_UP actions after SCALE_DOWN.
        """
        actions: List[ScaleAction] = []

        # The pool groups on the device
        device_pool_groups = {
            mid: dev_map[device.index]
            for mid, dev_map in self._request_pool_map.items()
            if device.index in dev_map
        }

        if model_id not in device_pool_groups:
            logger.warning(
                f"[Inference][Device-{device}] Model {model_id} not found on device."
            )
            return []

        device_id = device.index
        current_pg = device_pool_groups[model_id]
        current_num = len(current_pg.get_pool_ids())
        if current_num > 0:
            actions.append(
                ScaleAction(
                    ScaleActionType.SCALE_DOWN, current_num, model_id, device_id
                )
            )

        # case1: only one model on the device
        if len(device_pool_groups) == 1:
            return actions

        # case2: multiple models on the device, allocate memory to other models
        remaining_models = [mid for mid in device_pool_groups.keys() if mid != model_id]
        pool_nums = estimate_shared_pool_size_by_total_mem(
            device, remaining_models, None
        )
        if not pool_nums:
            logger.error(
                f"[Inference][Device-{device}] Failed to redistribute memory after uninstalling {model_id}."
            )
            return actions

        # check whether the remaining models need expansion
        for mid in remaining_models:
            pg = device_pool_groups[mid]
            current_num = len(pg.get_pool_ids())
            target_num = pool_nums[mid]
            diff = target_num - current_num
            if diff > 0:
                actions.append(
                    ScaleAction(ScaleActionType.SCALE_UP, diff, mid, device_id)
                )

        return actions
