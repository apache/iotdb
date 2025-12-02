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

import threading
import time
from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd
import torch
import torch.multiprocessing as mp

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import (
    InferenceModelInternalError,
    InvalidWindowArgumentError,
    NumericalRangeException,
    runtime_error_extractor,
)
from iotdb.ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from iotdb.ainode.core.inference.pool_controller import PoolController
from iotdb.ainode.core.inference.strategy.timer_sundial_inference_pipeline import (
    TimerSundialInferencePipeline,
)
from iotdb.ainode.core.inference.strategy.timerxl_inference_pipeline import (
    TimerXLInferencePipeline,
)
from iotdb.ainode.core.inference.utils import generate_req_id
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.model.model_enums import BuiltInModelType
from iotdb.ainode.core.model.sundial.configuration_sundial import SundialConfig
from iotdb.ainode.core.model.sundial.modeling_sundial import SundialForPrediction
from iotdb.ainode.core.model.timerxl.configuration_timer import TimerConfig
from iotdb.ainode.core.model.timerxl.modeling_timer import TimerForPrediction
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.gpu_mapping import get_available_devices
from iotdb.ainode.core.util.serde import convert_to_binary
from iotdb.thrift.ainode.ttypes import (
    TForecastReq,
    TForecastResp,
    TInferenceReq,
    TInferenceResp,
    TLoadModelReq,
    TShowLoadedModelsReq,
    TShowLoadedModelsResp,
    TUnloadModelReq,
)
from iotdb.thrift.common.ttypes import TSStatus
from iotdb.tsfile.utils.tsblock_serde import deserialize

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
            np_data = data.byteswap()
            data = np_data.view(np_data.dtype.newbyteorder())
        seqs = torch.tensor(data).unsqueeze(0).float()
        # TODO: unify model inference input
        output = self.model.generate(seqs, max_new_tokens=predict_length, revin=True)
        df = pd.DataFrame(output[0])
        return convert_to_binary(df)


class SundialStrategy(InferenceStrategy):
    def infer(self, full_data, predict_length=96, **_):
        data = full_data[1][0]
        if data.dtype.byteorder not in ("=", "|"):
            np_data = data.byteswap()
            data = np_data.view(np_data.dtype.newbyteorder())
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
    WAITING_INTERVAL_IN_MS = (
        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
    )  # How often to check for requests in the result queue

    def __init__(self):
        self._model_manager = ModelManager()
        self._model_mem_usage_map: Dict[str, int] = (
            {}
        )  # store model memory usage for each model
        self._result_queue = mp.Queue()
        self._result_wrapper_map = {}
        self._result_wrapper_lock = threading.RLock()

        self._stop_event = mp.Event()
        self._result_handler_thread = threading.Thread(
            target=self._handle_results, daemon=True
        )
        self._result_handler_thread.start()
        self._pool_controller = PoolController(self._result_queue)

    def load_model(self, req: TLoadModelReq) -> TSStatus:
        devices_to_be_processed = []
        devices_not_to_be_processed = []
        for device_id in req.deviceIdList:
            if self._pool_controller.has_request_pools(
                model_id=req.existingModelId, device_id=device_id
            ):
                devices_not_to_be_processed.append(device_id)
            else:
                devices_to_be_processed.append(device_id)
        if len(devices_to_be_processed) > 0:
            self._pool_controller.load_model(
                model_id=req.existingModelId, device_id_list=devices_to_be_processed
            )
        logger.info(
            f"[Inference] Start loading model [{req.existingModelId}] to devices [{devices_to_be_processed}], skipped devices [{devices_not_to_be_processed}] cause they have already loaded this model."
        )
        return TSStatus(
            code=TSStatusCode.SUCCESS_STATUS.value,
            message='Successfully submitted load model task, please use "SHOW LOADED MODELS" to check progress.',
        )

    def unload_model(self, req: TUnloadModelReq) -> TSStatus:
        devices_to_be_processed = []
        devices_not_to_be_processed = []
        for device_id in req.deviceIdList:
            if self._pool_controller.has_request_pools(
                model_id=req.modelId, device_id=device_id
            ):
                devices_to_be_processed.append(device_id)
            else:
                devices_not_to_be_processed.append(device_id)
        if len(devices_to_be_processed) > 0:
            self._pool_controller.unload_model(
                model_id=req.modelId, device_id_list=req.deviceIdList
            )
        logger.info(
            f"[Inference] Start unloading model [{req.modelId}] from devices [{devices_to_be_processed}], skipped devices [{devices_not_to_be_processed}] cause they haven't loaded this model."
        )
        return TSStatus(
            code=TSStatusCode.SUCCESS_STATUS.value,
            message='Successfully submitted unload model task, please use "SHOW LOADED MODELS" to check progress.',
        )

    def show_loaded_models(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
        return TShowLoadedModelsResp(
            status=get_status(TSStatusCode.SUCCESS_STATUS),
            deviceLoadedModelsMap=self._pool_controller.show_loaded_models(
                req.deviceIdList
                if len(req.deviceIdList) > 0
                else get_available_devices()
            ),
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

    def _process_request(self, req):
        req_id = req.req_id
        infer_proxy = InferenceRequestProxy(req_id)
        with self._result_wrapper_lock:
            self._result_wrapper_map[req_id] = infer_proxy
        try:
            # dispatch request to the pool
            self._pool_controller.add_request(req, infer_proxy)
            outputs = infer_proxy.wait_for_result()
            return outputs
        except Exception as e:
            logger.error(e)
            raise InferenceModelInternalError(str(e))
        finally:
            with self._result_wrapper_lock:
                del self._result_wrapper_map[req_id]

    def _get_strategy(self, model_id, model):
        if isinstance(model, TimerForPrediction):
            return TimerXLStrategy(model)
        if isinstance(model, SundialForPrediction):
            return SundialStrategy(model)
        if self._model_manager.model_storage.is_built_in_or_fine_tuned(model_id):
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

            predict_length = int(inference_attrs.pop("predict_length", 96))
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

            if self._pool_controller.has_request_pools(model_id):
                # use request pool to accelerate inference when the model instance is already loaded.
                # TODO: TSBlock -> Tensor codes should be unified
                data = full_data[1][0]
                if data.dtype.byteorder not in ("=", "|"):
                    np_data = data.byteswap()
                    data = np_data.view(np_data.dtype.newbyteorder())
                # the inputs should be on CPU before passing to the inference request
                inputs = torch.tensor(data).unsqueeze(0).float().to("cpu")
                model_type = self._model_manager.get_model_info(model_id).model_type
                if model_type == BuiltInModelType.SUNDIAL.value:
                    inference_pipeline = TimerSundialInferencePipeline(SundialConfig())
                elif model_type == BuiltInModelType.TIMER_XL.value:
                    inference_pipeline = TimerXLInferencePipeline(TimerConfig())
                else:
                    raise InferenceModelInternalError(
                        f"Unsupported model_id: {model_id}"
                    )
                infer_req = InferenceRequest(
                    req_id=generate_req_id(),
                    model_id=model_id,
                    inputs=inputs,
                    inference_pipeline=inference_pipeline,
                    max_new_tokens=predict_length,
                )
                outputs = self._process_request(infer_req)
                outputs = convert_to_binary(pd.DataFrame(outputs[0]))
            else:
                # load model
                accel = str(inference_attrs.get("acceleration", "")).lower() == "true"
                model = self._model_manager.load_model(model_id, inference_attrs, accel)
                # inference by strategy
                strategy = self._get_strategy(model_id, model)
                outputs = strategy.infer(
                    full_data, predict_length=predict_length, **inference_attrs
                )

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

    def stop(self):
        self._stop_event.set()
        self._pool_controller.stop()
        while not self._result_queue.empty():
            self._result_queue.get_nowait()
        self._result_queue.close()
