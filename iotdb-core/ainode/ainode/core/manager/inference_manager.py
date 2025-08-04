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
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, List

import pandas as pd
import psutil
import torch
import torch.multiprocessing as mp
from iotdb.tsfile.utils.tsblock_serde import deserialize

from ainode.core.config import AINodeDescriptor
from ainode.core.constant import TSStatusCode
from ainode.core.exception import (
    InferenceModelInternalError,
    InvalidWindowArgumentError,
    NumericalRangeException,
    runtime_error_extractor,
)
from ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from ainode.core.inference.inference_request_pool import InferenceRequestPool
from ainode.core.inference.strategy.timer_sundial_inference_pipeline import (
    TimerSundialInferencePipeline,
)
from ainode.core.inference.strategy.timerxl_inference_pipeline import (
    TimerXLInferencePipeline,
)
from ainode.core.inference.utils import generate_req_id
from ainode.core.log import Logger
from ainode.core.manager.model_manager import ModelManager
from ainode.core.model.sundial.configuration_sundial import SundialConfig
from ainode.core.model.sundial.modeling_sundial import SundialForPrediction
from ainode.core.model.timerxl.configuration_timer import TimerConfig
from ainode.core.model.timerxl.modeling_timer import TimerForPrediction
from ainode.core.rpc.status import get_status
from ainode.core.util.serde import convert_to_binary
from ainode.thrift.ainode.ttypes import (
    TForecastReq,
    TForecastResp,
    TInferenceReq,
    TInferenceResp,
)

logger = Logger()


class InferenceStrategy(ABC):
    def __init__(self, model):
        self.model = model

    @abstractmethod
    def infer(self, full_data, **kwargs):
        pass


# [IoTDB] full data deserialized from iotdb is composed of [timestampList, valueList, length],
# we only get valueList currently.
class TimerXLStrategy(InferenceStrategy):
    def infer(self, full_data, predict_length=96, **_):
        data = full_data[1][0]
        if data.dtype.byteorder not in ("=", "|"):
            data = data.byteswap().newbyteorder()
        seqs = torch.tensor(data).unsqueeze(0).float()
        # TODO: unify model inference input
        output = self.model.generate(seqs, max_new_tokens=predict_length, revin=True)
        df = pd.DataFrame(output[0])
        return convert_to_binary(df)


class SundialStrategy(InferenceStrategy):
    def infer(self, full_data, predict_length=96, **_):
        data = full_data[1][0]
        if data.dtype.byteorder not in ("=", "|"):
            data = data.byteswap().newbyteorder()
        seqs = torch.tensor(data).unsqueeze(0).float()
        # TODO: unify model inference input
        output = self.model.generate(
            seqs, max_new_tokens=predict_length, num_samples=10, revin=True
        )
        df = pd.DataFrame(output[0].mean(dim=0))
        return convert_to_binary(df)


class BuiltInStrategy(InferenceStrategy):
    def infer(self, full_data, **_):
        data = pd.DataFrame(full_data[1]).T
        output = self.model.inference(data)
        df = pd.DataFrame(output)
        return convert_to_binary(df)


class RegisteredStrategy(InferenceStrategy):
    def infer(self, full_data, window_interval=None, window_step=None, **_):
        _, dataset, _, length = full_data
        if window_interval is None or window_step is None:
            window_interval = length
            window_step = float("inf")

        if window_interval <= 0 or window_step <= 0 or window_interval > length:
            raise InvalidWindowArgumentError(window_interval, window_step, length)

        data = torch.tensor(dataset, dtype=torch.float32).unsqueeze(0).permute(0, 2, 1)

        times = int((length - window_interval) // window_step + 1)
        results = []
        try:
            for i in range(times):
                start = 0 if window_step == float("inf") else i * window_step
                end = start + window_interval
                window = data[:, start:end, :]
                out = self.model(window)
                df = pd.DataFrame(out.squeeze(0).detach().numpy())
                results.append(df)
        except Exception as e:
            msg = runtime_error_extractor(str(e)) or str(e)
            raise InferenceModelInternalError(msg)

        # concatenate or return first window for forecast
        return [convert_to_binary(df) for df in results]


class InferenceManager:
    ACCELERATE_MODEL_ID = ["sundial", "timer_xl"]
    DEFAULT_DEVICE = "cpu"
    # DEFAULT_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    WAITING_INTERVAL_IN_MS = (
        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
    )  # How often to check for requests in the result queue
    MODEL_MEM_USAGE_MAP = (
        AINodeDescriptor().get_config().get_ain_inference_model_mem_usage_map()
    )  # the memory usage of each model in bytes
    INFERENCE_MEMORY_USAGE_RATIO = (
        AINodeDescriptor().get_config().get_ain_inference_memory_usage_ratio()
    )  # the device space allocated for inference
    INFERENCE_WEIGHT_OVERHEAD_RATIO = (
        AINodeDescriptor().get_config().get_ain_inference_weight_overhead_ratio()
    )  # the weight overhead ratio for inference, used to estimate the pool size

    def __init__(self):
        self._model_manager = ModelManager()
        self._result_queue = mp.Queue()
        self._result_wrapper_map = {}
        self._result_wrapper_lock = threading.RLock()
        self._pool_init_lock = (
            threading.Lock()
        )  # used for lazy initialization of inference request pools
        # structure: {model_id: [(InferenceRequestPool, request_queue), ...]}
        self._request_pool_map: Dict[str, List[(InferenceRequestPool, mp.Queue)]] = {}
        self._stop_event = mp.Event()
        self._result_handler_thread = threading.Thread(
            target=self._handle_results, daemon=True
        )
        self._result_handler_thread.start()
        self._model_mem_usage_map: Dict[str, int] = (
            {}
        )  # store model memory usage for each model
        # self._preload_model_benchmarks()

    def _preload_model_benchmarks(self):
        if "cuda" in str(self.DEFAULT_DEVICE):
            model_id = self.ACCELERATE_MODEL_ID
            mem_usage = self._measure_model_memory(model_id)
            self._model_mem_usage_map[model_id] = mem_usage
            logger.info(
                f"[Inference] Preloaded benchmark for {model_id}, mem_usage={mem_usage/1024**2:.2f} MB"
            )
        else:
            logger.warning(
                f"[Inference] Skipped preloading benchmarks for {self.DEFAULT_DEVICE}, only supports CUDA currently"
            )

    def _measure_model_memory(self, model_id: str) -> int:
        # TODO: support CPU in the future
        # TODO: we can estimate the memory usage by running a dummy inference
        device = self.DEFAULT_DEVICE
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
        inference_factor = 1.2
        overhead = 500 * 1024 * 1024
        final = int(max(usage, 1) * inference_factor + overhead)
        return final

    def _evaluate_system_resources(self):
        if torch.cuda.is_available():
            free_mem, total_mem = torch.cuda.mem_get_info()
            logger.info(
                f"[Inference][Device-{self.DEFAULT_DEVICE}] CUDA device memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
            )
            return {"device": "cuda", "free_mem": free_mem, "total_mem": total_mem}
        else:
            # TODO: test cpu in the future
            free_mem = psutil.virtual_memory().available
            total_mem = psutil.virtual_memory().total
            logger.info(
                f"[Inference][Device-{self.DEFAULT_DEVICE}] CPU memory: free={free_mem/1024**2:.2f} MB, total={total_mem/1024**2:.2f} MB"
            )
            return {"device": "cpu", "free_mem": free_mem, "total_mem": total_mem}

    def _estimate_pool_size(self, model_id: str) -> int:
        if model_id not in self.MODEL_MEM_USAGE_MAP:
            logger.error(
                f"[Inference][Device-{self.DEFAULT_DEVICE}] Model {model_id} not supported now"
            )
            return 0

        system_res = self._evaluate_system_resources()
        free_mem = system_res["free_mem"]

        mem_usage = (
            self.MODEL_MEM_USAGE_MAP[model_id] * self.INFERENCE_WEIGHT_OVERHEAD_RATIO
        )
        size = int((free_mem * self.INFERENCE_MEMORY_USAGE_RATIO) // mem_usage)
        if size <= 0:
            logger.error(
                f"[Inference][Device-{self.DEFAULT_DEVICE}] Not enough memory to run model {model_id}. free={free_mem/1024**2:.2f} MB, need>={mem_usage/1024**2:.2f} MB"
            )
            return 0

        logger.info(
            f"[Inference][Device-{self.DEFAULT_DEVICE}] "
            f"model={model_id}, mem_usage={mem_usage/1024**2:.2f} MB, "
            f"pool_num={size}"
        )
        return size

    def _first_pool_init(self, model_id: str):
        if model_id == "sundial":
            config = SundialConfig()
        elif model_id == "timer_xl":
            config = TimerConfig()
        first_queue = mp.Queue()
        ready_event = mp.Event()
        first_pool = InferenceRequestPool(
            pool_id=0,
            model_id=model_id,
            config=config,
            request_queue=first_queue,
            result_queue=self._result_queue,
            ready_event=ready_event,
        )
        first_pool.start()
        if not ready_event.wait(timeout=30):
            logger.error(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-0] First pool failed to be ready in time"
            )
        else:
            self._request_pool_map[model_id] = [(first_pool, first_queue)]
            logger.info(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-0] Initialized inference request pool for model {model_id}"
            )

    def _expand_pools(self, model_id, start_idx, count):
        for idx in range(count):
            queue = mp.Queue()
            if model_id == "sundial":
                config = SundialConfig()
            elif model_id == "timer_xl":
                config = TimerConfig()
            pool = InferenceRequestPool(
                pool_id=start_idx + idx,
                model_id=model_id,
                config=config,
                request_queue=queue,
                result_queue=self._result_queue,
                ready_event=mp.Event(),
            )
            pool.start()
            self._request_pool_map[model_id].append((pool, queue))
            logger.info(
                f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool.pool_id}] New inference request pool started for model {model_id}"
            )

    def _handle_results(self):
        while not self._stop_event.is_set():
            if self._result_queue.empty():
                time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
                continue
            infer_req: InferenceRequest = self._result_queue.get()
            with self._result_wrapper_lock:
                self._result_wrapper_map[infer_req.req_id].set_result(
                    infer_req.get_final_output()
                )

    def _get_strategy(self, model_id, model):
        if isinstance(model, TimerForPrediction):
            return TimerXLStrategy(model)
        if isinstance(model, SundialForPrediction):
            return SundialStrategy(model)
        if self._model_manager.model_storage._is_built_in_or_fine_tuned(model_id):
            return BuiltInStrategy(model)
        return RegisteredStrategy(model)

    def _run(
        self,
        req,
        data_getter,
        deserializer,
        extract_attrs,
        resp_cls,
        single_output: bool,
    ):
        model_id = req.modelId
        try:
            raw = data_getter(req)
            full_data = deserializer(raw)
            inference_attrs = extract_attrs(req)

            predict_length = inference_attrs.get("predict_length", 96)
            if (
                predict_length
                > AINodeDescriptor().get_config().get_ain_inference_max_predict_length()
            ):
                raise NumericalRangeException(
                    "output_length",
                    1,
                    AINodeDescriptor()
                    .get_config()
                    .get_ain_inference_max_predict_length(),
                    predict_length,
                )

            if model_id in self.ACCELERATE_MODEL_ID and "cuda" in str(
                self.DEFAULT_DEVICE
            ):
                # lazy initialization for first request
                if model_id not in self._request_pool_map:
                    with self._pool_init_lock:
                        # cross-check to avoid multiple initializations
                        if model_id not in self._request_pool_map:
                            pool_num = self._estimate_pool_size(model_id)
                            if pool_num <= 0:
                                raise InferenceModelInternalError(
                                    f"Not enough memory to run model {model_id}."
                                )
                            # initialize the first pool
                            self._first_pool_init(model_id)
                            # start a background thread to expand pools
                            expand_thread = threading.Thread(
                                target=self._expand_pools,
                                args=(model_id, 1, pool_num - 1),
                                daemon=True,
                            )
                            expand_thread.start()
                # TODO: Logic in this branch shall handle all LSTM inferences
                # TODO: TSBlock -> Tensor codes should be unified
                data = full_data[1][0]
                if data.dtype.byteorder not in ("=", "|"):
                    data = data.byteswap().newbyteorder()
                # the inputs should be on CPU before passing to the inference request
                inputs = torch.tensor(data).unsqueeze(0).float().to("cpu")
                if model_id == "sundial":
                    inference_pipeline = TimerSundialInferencePipeline(SundialConfig())
                elif model_id == "timer_xl":
                    inference_pipeline = TimerXLInferencePipeline(TimerConfig())
                infer_req = InferenceRequest(
                    req_id=generate_req_id(),
                    inputs=inputs,
                    inference_pipeline=inference_pipeline,
                    max_new_tokens=predict_length,
                )
                infer_proxy = InferenceRequestProxy(infer_req.req_id)
                with self._result_wrapper_lock:
                    self._result_wrapper_map[infer_req.req_id] = infer_proxy
                pool_idx = hash(infer_req.req_id) % len(
                    self._request_pool_map[model_id]
                )
                self._request_pool_map[model_id][pool_idx][1].put(infer_req)
                logger.debug(
                    f"[Inference][Device-{self.DEFAULT_DEVICE}][Pool-{pool_idx}][ID-{infer_req.req_id}] Request is queued for inference"
                )
                outputs = infer_proxy.wait_for_completion()
                outputs = convert_to_binary(pd.DataFrame(outputs[0]))
                with self._result_wrapper_lock:
                    del self._result_wrapper_map[infer_req.req_id]
            else:
                # load model
                accel = str(inference_attrs.get("acceleration", "")).lower() == "true"
                model = self._model_manager.load_model(model_id, inference_attrs, accel)
                # inference by strategy
                strategy = self._get_strategy(model_id, model)
                outputs = strategy.infer(full_data, **inference_attrs)

            # construct response
            status = get_status(TSStatusCode.SUCCESS_STATUS)

            if isinstance(outputs, list):
                return resp_cls(status, outputs[0] if single_output else outputs)
            return resp_cls(status, outputs if single_output else [outputs])

        except Exception as e:
            logger.error(e)
            status = get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            empty = b"" if single_output else []
            return resp_cls(status, empty)

    def forecast(self, req: TForecastReq):
        return self._run(
            req,
            data_getter=lambda r: r.inputData,
            deserializer=deserialize,
            extract_attrs=lambda r: {
                "predict_length": r.outputLength,
                **(r.options or {}),
            },
            resp_cls=TForecastResp,
            single_output=True,
        )

    def inference(self, req: TInferenceReq):
        return self._run(
            req,
            data_getter=lambda r: r.dataset,
            deserializer=deserialize,
            extract_attrs=lambda r: {
                "window_interval": getattr(r.windowParams, "windowInterval", None),
                "window_step": getattr(r.windowParams, "windowStep", None),
                **(r.inferenceAttributes or {}),
            },
            resp_cls=TInferenceResp,
            single_output=False,
        )

    def shutdown(self):
        self._stop_event.set()
        for model_id, pools in self._request_pool_map.items():
            for requestPool, requestQueue in pools:
                requestPool.stop()
                while not requestQueue.empty():
                    requestQueue.get_nowait()
                requestQueue.close()
            for requestPool, _ in pools:
                requestPool.join(timeout=10)
        while not self._result_queue.empty():
            self._result_queue.get_nowait()
        self._result_queue.close()
