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
1import gc
1
1import psutil
1import torch
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.exception import ModelNotExistError
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.manager.model_manager import ModelManager
1from iotdb.ainode.core.model.model_info import BUILT_IN_LTSM_MAP
1
1logger = Logger()
1
1MODEL_MEM_USAGE_MAP = (
1    AINodeDescriptor().get_config().get_ain_inference_model_mem_usage_map()
1)  # the memory usage of each model in bytes
1INFERENCE_MEMORY_USAGE_RATIO = (
1    AINodeDescriptor().get_config().get_ain_inference_memory_usage_ratio()
1)  # the device space allocated for inference
1INFERENCE_EXTRA_MEMORY_RATIO = (
1    AINodeDescriptor().get_config().get_ain_inference_extra_memory_ratio()
1)  # the overhead ratio for inference, used to estimate the pool size
1
1
1def measure_model_memory(device: torch.device, model_id: str) -> int:
1    # TODO: support CPU in the future
1    # TODO: we can estimate the memory usage by running a dummy inference
1    torch.cuda.empty_cache()
1    torch.cuda.synchronize(device)
1    start = torch.cuda.memory_reserved(device)
1
1    model = ModelManager().load_model(model_id, {}).to(device)
1    torch.cuda.synchronize(device)
1    end = torch.cuda.memory_reserved(device)
1    usage = end - start
1
1    # delete model to free memory
1    del model
1    torch.cuda.empty_cache()
1    gc.collect()
1
1    # add inference factor and cuda context overhead
1    overhead = 500 * 1024**2  # 500 MiB
1    final = int(max(usage, 1) * INFERENCE_EXTRA_MEMORY_RATIO + overhead)
1    return final
1
1
1def evaluate_system_resources(device: torch.device) -> dict:
1    if torch.cuda.is_available():
1        free_mem, total_mem = torch.cuda.mem_get_info()
1        logger.info(
1            f"[Inference][Device-{device}] CUDA device memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
1        )
1        return {"device": "cuda", "free_mem": free_mem, "total_mem": total_mem}
1    else:
1        free_mem = psutil.virtual_memory().available
1        total_mem = psutil.virtual_memory().total
1        logger.info(
1            f"[Inference][Device-{device}] CPU memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
1        )
1        return {"device": "cpu", "free_mem": free_mem, "total_mem": total_mem}
1
1
1def estimate_pool_size(device: torch.device, model_id: str) -> int:
1    model_info = BUILT_IN_LTSM_MAP.get(model_id, None)
1    if model_info is None or model_info.model_type not in MODEL_MEM_USAGE_MAP:
1        logger.error(
1            f"[Inference] Cannot estimate inference pool size on device: {device}, because model: {model_id} is not supported."
1        )
1        raise ModelNotExistError(model_id)
1
1    system_res = evaluate_system_resources(device)
1    free_mem = system_res["free_mem"]
1
1    mem_usage = (
1        MODEL_MEM_USAGE_MAP[model_info.model_type] * INFERENCE_EXTRA_MEMORY_RATIO
1    )
1    size = int((free_mem * INFERENCE_MEMORY_USAGE_RATIO) // mem_usage)
1    if size <= 0:
1        logger.error(
1            f"[Inference][Device-{device}] Not enough memory to run model {model_id}. free={free_mem/1024**2:.2f} MB, need>={mem_usage/1024**2:.2f} MB"
1        )
1        return 0
1
1    logger.info(
1        f"[Inference][Device-{device}] "
1        f"model={model_id}, mem_usage={mem_usage/1024**2:.2f} MB, "
1        f"pool_num={size}"
1    )
1    return size
1