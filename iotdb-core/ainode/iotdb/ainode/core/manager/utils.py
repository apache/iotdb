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

import psutil
import torch

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.exception import ModelNotExistException
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.model.model_loader import load_model

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


def evaluate_system_resources(device: torch.device) -> dict:
    if device.type == "cuda":
        free_mem, total_mem = torch.cuda.mem_get_info()
        logger.info(
            f"[Inference][{device}] CUDA device memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
        )
        return {"device": "cuda", "free_mem": free_mem, "total_mem": total_mem}
    else:
        free_mem = psutil.virtual_memory().available
        total_mem = psutil.virtual_memory().total
        logger.info(
            f"[Inference][{device}] CPU memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
        )
        return {"device": "cpu", "free_mem": free_mem, "total_mem": total_mem}


def estimate_pool_size(device: torch.device, model_id: str) -> int:
    model_info = ModelManager().get_model_info(model_id)
    if model_info is None or model_info.model_type not in MODEL_MEM_USAGE_MAP:
        logger.error(
            f"[Inference] Cannot estimate inference pool size on device: {device}, because model: {model_id} is not supported."
        )
        raise ModelNotExistException(model_id)

    system_res = evaluate_system_resources(device)
    free_mem = system_res["free_mem"]

    mem_usage = (
        MODEL_MEM_USAGE_MAP[model_info.model_type] * INFERENCE_EXTRA_MEMORY_RATIO
    )
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
