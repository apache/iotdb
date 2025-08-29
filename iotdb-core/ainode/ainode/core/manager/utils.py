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

import gc
from typing import Dict, List, Optional

import psutil
import torch

from ainode.core.config import AINodeDescriptor
from ainode.core.log import Logger
from ainode.core.manager.model_manager import ModelManager
from ainode.core.model.model_info import BUILT_IN_LTSM_MAP

logger = Logger()

MODEL_MEM_USAGE_MAP = (
    AINodeDescriptor().get_config().get_ain_inference_model_mem_usage_map()
)  # the memory usage of each model in bytes
INFERENCE_MEMORY_USAGE_RATIO = (
    AINodeDescriptor().get_config().get_ain_inference_memory_usage_ratio()
)  # the device space allocated for inference
INFERENCE_EXTRA_MEMORY_RATIO = (
    AINodeDescriptor().get_config().get_ain_inference_extra_memory_ratio()
)  # the overhead ratio for inference, used to estimate the pool size


def measure_model_memory(device: torch.device, model_id: str) -> int:
    # TODO: support CPU in the future
    # TODO: we can estimate the memory usage by running a dummy inference
    torch.cuda.empty_cache()
    torch.cuda.synchronize(device)
    start = torch.cuda.memory_reserved(device)

    model = ModelManager().load_model(model_id, {}).to(device)
    torch.cuda.synchronize(device)
    end = torch.cuda.memory_reserved(device)
    usage = end - start

    # delete model to free memory
    del model
    torch.cuda.empty_cache()
    gc.collect()

    # add inference factor and cuda context overhead
    overhead = 500 * 1024**2  # 500 MiB
    final = int(max(usage, 1) * INFERENCE_EXTRA_MEMORY_RATIO + overhead)
    return final


def evaluate_system_resources(device: torch.device) -> dict:
    if torch.cuda.is_available():
        free_mem, total_mem = torch.cuda.mem_get_info()
        logger.info(
            f"[Inference][Device-{device}] CUDA device memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
        )
        return {"device": "cuda", "free_mem": free_mem, "total_mem": total_mem}
    else:
        # TODO: test cpu in the future
        free_mem = psutil.virtual_memory().available
        total_mem = psutil.virtual_memory().total
        logger.info(
            f"[Inference][Device-{device}] CPU memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
        )
        return {"device": "cpu", "free_mem": free_mem, "total_mem": total_mem}


def estimate_pool_size(device: torch.device, model_id: str) -> int:
    model_info = BUILT_IN_LTSM_MAP.get(model_id, None)
    if model_info is None:
        logger.error(f"[Inference][Device-{device}] Model {model_id} not found")
        return 0

    model_type = model_info.model_type
    if model_type not in MODEL_MEM_USAGE_MAP:
        logger.error(f"[Inference][Device-{device}] Model {model_id} not supported now")
        return 0

    system_res = evaluate_system_resources(device)
    free_mem = system_res["free_mem"]

    mem_usage = MODEL_MEM_USAGE_MAP[model_type] * INFERENCE_EXTRA_MEMORY_RATIO
    size = int((free_mem * INFERENCE_MEMORY_USAGE_RATIO) // mem_usage)
    if size <= 0:
        logger.error(
            f"[Inference][Device-{device}] Not enough memory to run model {model_id}. free={free_mem/1024**2:.2f} MB, need>={mem_usage/1024**2:.2f} MB"
        )
        return 0

    logger.info(
        f"[Inference][Device-{device}] "
        f"model={model_id}, mem_usage={mem_usage/1024**2:.2f} MB, "
        f"pool_num={size}"
    )
    return size


def estimate_shared_pool_size_by_total_mem(
    device: torch.device, existing_model_ids: List[str], new_model_id: Optional[str]
) -> Dict[str, int]:
    """
    Estimate pool counts for (existing_model_ids + new_model_id) by equally
    splitting the device's TOTAL memory among models.

    Returns:
        mapping {model_id: pool_num}
    """
    # Extract unique model IDs
    all_models = []
    for mid in existing_model_ids + [new_model_id]:
        if mid not in all_models:
            all_models.append(mid)

    # Seize memory usage for each model
    mem_usages: Dict[str, float] = {}
    for mid in all_models:
        model_info = BUILT_IN_LTSM_MAP.get(mid)
        if model_info is None:
            logger.error(f"[Inference][Device-{device}] Model {mid} not found")
            return {}
        model_type = model_info.model_type
        if model_type not in MODEL_MEM_USAGE_MAP:
            logger.error(f"[Inference][Device-{device}] Model {mid} not supported now")
            return {}

        mem_usages[mid] = MODEL_MEM_USAGE_MAP[model_type] * INFERENCE_EXTRA_MEMORY_RATIO

    # Evaluate system resources and get TOTAL memory
    system_res = evaluate_system_resources(device)
    # TODO: Its better to consider free memory, but we need to track the memory usage of existing pools
    total_mem = system_res.get("total_mem")

    usable_mem = total_mem * INFERENCE_MEMORY_USAGE_RATIO
    if usable_mem <= 0:
        logger.error(
            f"[Inference][Device-{device}] No usable memory on device. total={total_mem/1024**2:.2f} MB"
        )

    # Each model gets an equal share of the TOTAL memory
    num_models = len(all_models)
    per_model_share = usable_mem / num_models  # TODO: Implement more strategies later

    # Calculate pool allocation for each model
    allocation: Dict[str, int] = {}
    for mid in all_models:
        pool_num = int(per_model_share // mem_usages[mid])
        if pool_num <= 0:
            logger.error(
                f"[Inference][Device-{device}] Not enough TOTAL memory to guarantee at least 1 pool for model {mid}. "
                f"Per-model share={per_model_share/1024**2:.2f} MB, need>={mem_usages[mid]/1024**2:.2f} MB"
            )
        else:
            allocation[mid] = pool_num

    logger.info(
        f"[Inference][Device-{device}] Shared pool allocation (by TOTAL memory): {allocation}"
    )
    return allocation
