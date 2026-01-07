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

from typing import Dict, List, Optional

import torch

from iotdb.ainode.core.inference.pool_group import PoolGroup
from iotdb.ainode.core.inference.pool_scheduler.abstract_pool_scheduler import (
    AbstractPoolScheduler,
    ScaleAction,
    ScaleActionType,
)
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.manager.utils import (
    INFERENCE_EXTRA_MEMORY_RATIO,
    INFERENCE_MEMORY_USAGE_RATIO,
    MODEL_MEM_USAGE_MAP,
    evaluate_system_resources,
)
from iotdb.ainode.core.model.model_info import ModelInfo

logger = Logger()


def _estimate_shared_pool_size_by_total_mem(
    device: torch.device,
    existing_model_infos: List[ModelInfo],
    new_model_info: Optional[ModelInfo] = None,
) -> Dict[str, int]:
    """
    Estimate pool counts for (existing_model_ids + new_model_id) by equally
    splitting the device's TOTAL memory among models.

    Returns:
        mapping {model_id: pool_num}
    """
    # Extract unique model IDs
    all_models = existing_model_infos + (
        [new_model_info] if new_model_info is not None else []
    )

    # Seize memory usage for each model
    mem_usages: Dict[str, float] = {}
    for model_info in all_models:
        mem_usages[model_info.model_id] = (
            MODEL_MEM_USAGE_MAP[model_info.model_type] * INFERENCE_EXTRA_MEMORY_RATIO
        )

    # Evaluate system resources and get TOTAL memory
    system_res = evaluate_system_resources(device)
    # TODO: Its better to consider free memory, but we need to track the memory usage of existing pools
    total_mem = system_res.get("total_mem")

    usable_mem = total_mem * INFERENCE_MEMORY_USAGE_RATIO
    if usable_mem <= 0:
        logger.error(
            f"[Inference][{device}] No usable memory on device. total={total_mem / 1024 ** 2:.2f} MB, usable={usable_mem / 1024 ** 2:.2f} MB"
        )

    # Each model gets an equal share of the TOTAL memory
    num_models = len(all_models)
    per_model_share = usable_mem / num_models  # TODO: Implement more strategies later

    # Calculate pool allocation for each model
    allocation: Dict[str, int] = {}
    for model_info in all_models:
        pool_num = int(per_model_share // mem_usages[model_info.model_id])
        if pool_num <= 0:
            logger.warning(
                f"[Inference][{device}] Not enough TOTAL memory to guarantee at least 1 pool for model {model_info.model_id}, no pool will be scheduled for this model. "
                f"Per-model share={per_model_share / 1024 ** 2:.2f} MB, need>={mem_usages[model_info.model_id] / 1024 ** 2:.2f} MB"
            )
        allocation[model_info.model_id] = pool_num
    logger.info(
        f"[Inference][{device}] Shared pool allocation (by TOTAL memory): {allocation}"
    )
    return allocation


class BasicPoolScheduler(AbstractPoolScheduler):
    """
    A basic scheduler to init the request pools. In short,
    different kind of models will equally share the available resource of the located device,
    and scale down actions are always ahead of scale up.
    """

    def __init__(self, request_pool_map: Dict[str, Dict[torch.device, PoolGroup]]):
        super().__init__(request_pool_map)
        self._model_manager = ModelManager()

    def schedule(self, model_id: str) -> List[ScaleAction]:
        pass

    def schedule_load_model_to_device(
        self, model_info: ModelInfo, device_id: torch.device
    ) -> List[ScaleAction]:
        existing_model_infos = [
            self._model_manager.get_model_info(existing_model_id)
            for existing_model_id, pool_group_map in self._request_pool_map.items()
            if existing_model_id != model_info.model_id and device_id in pool_group_map
        ]
        allocation_result = _estimate_shared_pool_size_by_total_mem(
            device=device_id,
            existing_model_infos=existing_model_infos,
            new_model_info=model_info,
        )
        return self._convert_allocation_result_to_scale_actions(
            allocation_result, device_id
        )

    def schedule_unload_model_from_device(
        self, model_info: ModelInfo, device_id: torch.device
    ) -> List[ScaleAction]:
        existing_model_infos = [
            self._model_manager.get_model_info(existing_model_id)
            for existing_model_id, pool_group_map in self._request_pool_map.items()
            if existing_model_id != model_info.model_id and device_id in pool_group_map
        ]
        allocation_result = (
            _estimate_shared_pool_size_by_total_mem(
                device=device_id,
                existing_model_infos=existing_model_infos,
                new_model_info=None,
            )
            if len(existing_model_infos) > 0
            else {model_info.model_id: 0}
        )
        if len(existing_model_infos) > 0:
            allocation_result[model_info.model_id] = 0
        return self._convert_allocation_result_to_scale_actions(
            allocation_result, device_id
        )

    def _convert_allocation_result_to_scale_actions(
        self, allocation_result: Dict[str, int], device_id: torch.device
    ) -> List[ScaleAction]:
        """
        Convert the model allocation result to List[ScaleAction],
        where the scale down actions are always ahead of the scale up.
        """
        actions = []
        for model_id, target_num in allocation_result.items():
            current_num = self._request_pool_map.get(model_id, {}).get(device_id, None)
            current_num = current_num.get_pool_count() if current_num else 0
            diff = target_num - current_num
            if diff > 0:
                actions.append(
                    ScaleAction(
                        action=ScaleActionType.SCALE_UP,
                        amount=diff,
                        model_id=model_id,
                        device_id=device_id,
                    )
                )
            elif diff < 0:
                actions.append(
                    ScaleAction(
                        action=ScaleActionType.SCALE_DOWN,
                        amount=-diff,
                        model_id=model_id,
                        device_id=device_id,
                    )
                )
        sorted_actions = sorted(
            actions, key=lambda a: (0 if a.action == ScaleActionType.SCALE_DOWN else 1)
        )
        return sorted_actions
