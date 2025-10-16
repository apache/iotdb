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
1
1from typing import Dict, List, Optional
1
1import torch
1
1from iotdb.ainode.core.exception import InferenceModelInternalError
1from iotdb.ainode.core.inference.pool_group import PoolGroup
1from iotdb.ainode.core.inference.pool_scheduler.abstract_pool_scheduler import (
1    AbstractPoolScheduler,
1    ScaleAction,
1    ScaleActionType,
1)
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.manager.utils import (
1    INFERENCE_EXTRA_MEMORY_RATIO,
1    INFERENCE_MEMORY_USAGE_RATIO,
1    MODEL_MEM_USAGE_MAP,
1    estimate_pool_size,
1    evaluate_system_resources,
1)
1from iotdb.ainode.core.model.model_info import BUILT_IN_LTSM_MAP
1from iotdb.ainode.core.util.gpu_mapping import convert_device_id_to_torch_device
1
1logger = Logger()
1
1
1def _estimate_shared_pool_size_by_total_mem(
1    device: torch.device,
1    existing_model_ids: List[str],
1    new_model_id: Optional[str] = None,
1) -> Dict[str, int]:
1    """
1    Estimate pool counts for (existing_model_ids + new_model_id) by equally
1    splitting the device's TOTAL memory among models.
1
1    Returns:
1        mapping {model_id: pool_num}
1    """
1    # Extract unique model IDs
1    all_models = existing_model_ids + (
1        [new_model_id] if new_model_id is not None else []
1    )
1
1    # Seize memory usage for each model
1    mem_usages: Dict[str, float] = {}
1    for model_id in all_models:
1        model_info = BUILT_IN_LTSM_MAP.get(model_id)
1        model_type = model_info.model_type
1        mem_usages[model_id] = (
1            MODEL_MEM_USAGE_MAP[model_type] * INFERENCE_EXTRA_MEMORY_RATIO
1        )
1
1    # Evaluate system resources and get TOTAL memory
1    system_res = evaluate_system_resources(device)
1    # TODO: Its better to consider free memory, but we need to track the memory usage of existing pools
1    total_mem = system_res.get("total_mem")
1
1    usable_mem = total_mem * INFERENCE_MEMORY_USAGE_RATIO
1    if usable_mem <= 0:
1        logger.error(
1            f"[Inference][Device-{device}] No usable memory on device. total={total_mem / 1024 ** 2:.2f} MB, usable={usable_mem / 1024 ** 2:.2f} MB"
1        )
1
1    # Each model gets an equal share of the TOTAL memory
1    num_models = len(all_models)
1    per_model_share = usable_mem / num_models  # TODO: Implement more strategies later
1
1    # Calculate pool allocation for each model
1    allocation: Dict[str, int] = {}
1    for model_id in all_models:
1        pool_num = int(per_model_share // mem_usages[model_id])
1        if pool_num <= 0:
1            logger.warning(
1                f"[Inference][Device-{device}] Not enough TOTAL memory to guarantee at least 1 pool for model {model_id}, no pool will be scheduled for this model. "
1                f"Per-model share={per_model_share / 1024 ** 2:.2f} MB, need>={mem_usages[model_id] / 1024 ** 2:.2f} MB"
1            )
1        allocation[model_id] = pool_num
1    logger.info(
1        f"[Inference][Device-{device}] Shared pool allocation (by TOTAL memory): {allocation}"
1    )
1    return allocation
1
1
1class BasicPoolScheduler(AbstractPoolScheduler):
1    """
1    A basic scheduler to init the request pools. In short, different kind of models will equally share the available resource of the located device, and scale down actions are always ahead of scale up.
1    """
1
1    def __init__(self, request_pool_map: Dict[str, Dict[str, PoolGroup]]):
1        super().__init__(request_pool_map)
1
1    def schedule(self, model_id: str) -> List[ScaleAction]:
1        """
1        Schedule a scaling action for the given model_id.
1        """
1        if model_id not in self._request_pool_map:
1            pool_num = estimate_pool_size(self.DEFAULT_DEVICE, model_id)
1            if pool_num <= 0:
1                raise InferenceModelInternalError(
1                    f"Not enough memory to run model {model_id}."
1                )
1            return [ScaleAction(ScaleActionType.SCALE_UP, pool_num, model_id)]
1
1    def schedule_load_model_to_device(
1        self, model_id: str, device_id: str
1    ) -> List[ScaleAction]:
1        existing_model_ids = [
1            existing_model_id
1            for existing_model_id, pool_group_map in self._request_pool_map.items()
1            if existing_model_id != model_id and device_id in pool_group_map
1        ]
1        allocation_result = _estimate_shared_pool_size_by_total_mem(
1            device=convert_device_id_to_torch_device(device_id),
1            existing_model_ids=existing_model_ids,
1            new_model_id=model_id,
1        )
1        return self._convert_allocation_result_to_scale_actions(
1            allocation_result, device_id
1        )
1
1    def schedule_unload_model_from_device(
1        self, model_id: str, device_id: str
1    ) -> List[ScaleAction]:
1        existing_model_ids = [
1            existing_model_id
1            for existing_model_id, pool_group_map in self._request_pool_map.items()
1            if existing_model_id != model_id and device_id in pool_group_map
1        ]
1        allocation_result = (
1            _estimate_shared_pool_size_by_total_mem(
1                device=convert_device_id_to_torch_device(device_id),
1                existing_model_ids=existing_model_ids,
1                new_model_id=None,
1            )
1            if len(existing_model_ids) > 0
1            else {model_id: 0}
1        )
1        return self._convert_allocation_result_to_scale_actions(
1            allocation_result, device_id
1        )
1
1    def _convert_allocation_result_to_scale_actions(
1        self, allocation_result: Dict[str, int], device_id: str
1    ) -> List[ScaleAction]:
1        """
1        Convert the model allocation result to List[ScaleAction], where the scale down actions are always ahead of the scale up.
1        """
1        actions = []
1        for model_id, target_num in allocation_result.items():
1            current_num = self._request_pool_map.get(model_id, {}).get(device_id, None)
1            current_num = current_num.get_pool_count() if current_num else 0
1            diff = target_num - current_num
1            if diff > 0:
1                actions.append(
1                    ScaleAction(
1                        action=ScaleActionType.SCALE_UP,
1                        amount=diff,
1                        model_id=model_id,
1                        device_id=device_id,
1                    )
1                )
1            elif diff < 0:
1                actions.append(
1                    ScaleAction(
1                        action=ScaleActionType.SCALE_DOWN,
1                        amount=-diff,
1                        model_id=model_id,
1                        device_id=device_id,
1                    )
1                )
1        sorted_actions = sorted(
1            actions, key=lambda a: (0 if a.action == ScaleActionType.SCALE_DOWN else 1)
1        )
1        return sorted_actions
1