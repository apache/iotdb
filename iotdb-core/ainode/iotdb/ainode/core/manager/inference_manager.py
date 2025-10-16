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
1import threading
1import time
1from abc import ABC, abstractmethod
1from typing import Dict
1
1import pandas as pd
1import torch
1import torch.multiprocessing as mp
1
1from iotdb.ainode.core.config import AINodeDescriptor
1from iotdb.ainode.core.constant import TSStatusCode
1from iotdb.ainode.core.exception import (
1    InferenceModelInternalError,
1    InvalidWindowArgumentError,
1    NumericalRangeException,
1    runtime_error_extractor,
1)
1from iotdb.ainode.core.inference.inference_request import (
1    InferenceRequest,
1    InferenceRequestProxy,
1)
1from iotdb.ainode.core.inference.pool_controller import PoolController
1from iotdb.ainode.core.inference.strategy.timer_sundial_inference_pipeline import (
1    TimerSundialInferencePipeline,
1)
1from iotdb.ainode.core.inference.strategy.timerxl_inference_pipeline import (
1    TimerXLInferencePipeline,
1)
1from iotdb.ainode.core.inference.utils import generate_req_id
1from iotdb.ainode.core.log import Logger
1from iotdb.ainode.core.manager.model_manager import ModelManager
1from iotdb.ainode.core.model.sundial.configuration_sundial import SundialConfig
1from iotdb.ainode.core.model.sundial.modeling_sundial import SundialForPrediction
1from iotdb.ainode.core.model.timerxl.configuration_timer import TimerConfig
1from iotdb.ainode.core.model.timerxl.modeling_timer import TimerForPrediction
1from iotdb.ainode.core.rpc.status import get_status
1from iotdb.ainode.core.util.gpu_mapping import get_available_devices
1from iotdb.ainode.core.util.serde import convert_to_binary
1from iotdb.thrift.ainode.ttypes import (
1    TForecastReq,
1    TForecastResp,
1    TInferenceReq,
1    TInferenceResp,
1    TLoadModelReq,
1    TShowLoadedModelsReq,
1    TShowLoadedModelsResp,
1    TUnloadModelReq,
1)
1from iotdb.thrift.common.ttypes import TSStatus
1from iotdb.tsfile.utils.tsblock_serde import deserialize
1
1logger = Logger()
1
1
1class InferenceStrategy(ABC):
1    def __init__(self, model):
1        self.model = model
1
1    @abstractmethod
1    def infer(self, full_data, **kwargs):
1        pass
1
1
1# [IoTDB] full data deserialized from iotdb is composed of [timestampList, valueList, length],
1# we only get valueList currently.
1class TimerXLStrategy(InferenceStrategy):
1    def infer(self, full_data, predict_length=96, **_):
1        data = full_data[1][0]
1        if data.dtype.byteorder not in ("=", "|"):
1            np_data = data.byteswap()
1            data = np_data.view(np_data.dtype.newbyteorder())
1        seqs = torch.tensor(data).unsqueeze(0).float()
1        # TODO: unify model inference input
1        output = self.model.generate(seqs, max_new_tokens=predict_length, revin=True)
1        df = pd.DataFrame(output[0])
1        return convert_to_binary(df)
1
1
1class SundialStrategy(InferenceStrategy):
1    def infer(self, full_data, predict_length=96, **_):
1        data = full_data[1][0]
1        if data.dtype.byteorder not in ("=", "|"):
1            np_data = data.byteswap()
1            data = np_data.view(np_data.dtype.newbyteorder())
1        seqs = torch.tensor(data).unsqueeze(0).float()
1        # TODO: unify model inference input
1        output = self.model.generate(
1            seqs, max_new_tokens=predict_length, num_samples=10, revin=True
1        )
1        df = pd.DataFrame(output[0].mean(dim=0))
1        return convert_to_binary(df)
1
1
1class BuiltInStrategy(InferenceStrategy):
1    def infer(self, full_data, **_):
1        data = pd.DataFrame(full_data[1]).T
1        output = self.model.inference(data)
1        df = pd.DataFrame(output)
1        return convert_to_binary(df)
1
1
1class RegisteredStrategy(InferenceStrategy):
1    def infer(self, full_data, window_interval=None, window_step=None, **_):
1        _, dataset, _, length = full_data
1        if window_interval is None or window_step is None:
1            window_interval = length
1            window_step = float("inf")
1
1        if window_interval <= 0 or window_step <= 0 or window_interval > length:
1            raise InvalidWindowArgumentError(window_interval, window_step, length)
1
1        data = torch.tensor(dataset, dtype=torch.float32).unsqueeze(0).permute(0, 2, 1)
1
1        times = int((length - window_interval) // window_step + 1)
1        results = []
1        try:
1            for i in range(times):
1                start = 0 if window_step == float("inf") else i * window_step
1                end = start + window_interval
1                window = data[:, start:end, :]
1                out = self.model(window)
1                df = pd.DataFrame(out.squeeze(0).detach().numpy())
1                results.append(df)
1        except Exception as e:
1            msg = runtime_error_extractor(str(e)) or str(e)
1            raise InferenceModelInternalError(msg)
1
1        # concatenate or return first window for forecast
1        return [convert_to_binary(df) for df in results]
1
1
1class InferenceManager:
1    WAITING_INTERVAL_IN_MS = (
1        AINodeDescriptor().get_config().get_ain_inference_batch_interval_in_ms()
1    )  # How often to check for requests in the result queue
1
1    def __init__(self):
1        self._model_manager = ModelManager()
1        self._model_mem_usage_map: Dict[str, int] = (
1            {}
1        )  # store model memory usage for each model
1        self._result_queue = mp.Queue()
1        self._result_wrapper_map = {}
1        self._result_wrapper_lock = threading.RLock()
1
1        self._stop_event = mp.Event()
1        self._result_handler_thread = threading.Thread(
1            target=self._handle_results, daemon=True
1        )
1        self._result_handler_thread.start()
1        self._pool_controller = PoolController(self._result_queue)
1
1    def load_model(self, req: TLoadModelReq) -> TSStatus:
1        devices_to_be_processed = []
1        devices_not_to_be_processed = []
1        for device_id in req.deviceIdList:
1            if self._pool_controller.has_request_pools(
1                model_id=req.existingModelId, device_id=device_id
1            ):
1                devices_not_to_be_processed.append(device_id)
1            else:
1                devices_to_be_processed.append(device_id)
1        if len(devices_to_be_processed) > 0:
1            self._pool_controller.load_model(
1                model_id=req.existingModelId, device_id_list=devices_to_be_processed
1            )
1        logger.info(
1            f"[Inference] Start loading model [{req.existingModelId}] to devices [{devices_to_be_processed}], skipped devices [{devices_not_to_be_processed}] cause they have already loaded this model."
1        )
1        return TSStatus(
1            code=TSStatusCode.SUCCESS_STATUS.value,
1            message='Successfully submitted load model task, please use "SHOW LOADED MODELS" to check progress.',
1        )
1
1    def unload_model(self, req: TUnloadModelReq) -> TSStatus:
1        devices_to_be_processed = []
1        devices_not_to_be_processed = []
1        for device_id in req.deviceIdList:
1            if self._pool_controller.has_request_pools(
1                model_id=req.modelId, device_id=device_id
1            ):
1                devices_to_be_processed.append(device_id)
1            else:
1                devices_not_to_be_processed.append(device_id)
1        if len(devices_to_be_processed) > 0:
1            self._pool_controller.unload_model(
1                model_id=req.modelId, device_id_list=req.deviceIdList
1            )
1        logger.info(
1            f"[Inference] Start unloading model [{req.modelId}] from devices [{devices_to_be_processed}], skipped devices [{devices_not_to_be_processed}] cause they haven't loaded this model."
1        )
1        return TSStatus(
1            code=TSStatusCode.SUCCESS_STATUS.value,
1            message='Successfully submitted unload model task, please use "SHOW LOADED MODELS" to check progress.',
1        )
1
1    def show_loaded_models(self, req: TShowLoadedModelsReq) -> TShowLoadedModelsResp:
1        return TShowLoadedModelsResp(
1            status=get_status(TSStatusCode.SUCCESS_STATUS),
1            deviceLoadedModelsMap=self._pool_controller.show_loaded_models(
1                req.deviceIdList
1                if len(req.deviceIdList) > 0
1                else get_available_devices()
1            ),
1        )
1
1    def _handle_results(self):
1        while not self._stop_event.is_set():
1            if self._result_queue.empty():
1                time.sleep(self.WAITING_INTERVAL_IN_MS / 1000)
1                continue
1            infer_req: InferenceRequest = self._result_queue.get()
1            with self._result_wrapper_lock:
1                self._result_wrapper_map[infer_req.req_id].set_result(
1                    infer_req.get_final_output()
1                )
1
1    def _process_request(self, req):
1        req_id = req.req_id
1        infer_proxy = InferenceRequestProxy(req_id)
1        with self._result_wrapper_lock:
1            self._result_wrapper_map[req_id] = infer_proxy
1        try:
1            # dispatch request to the pool
1            self._pool_controller.add_request(req, infer_proxy)
1            outputs = infer_proxy.wait_for_result()
1            return outputs
1        except Exception as e:
1            logger.error(e)
1            raise InferenceModelInternalError(str(e))
1        finally:
1            with self._result_wrapper_lock:
1                del self._result_wrapper_map[req_id]
1
1    def _get_strategy(self, model_id, model):
1        if isinstance(model, TimerForPrediction):
1            return TimerXLStrategy(model)
1        if isinstance(model, SundialForPrediction):
1            return SundialStrategy(model)
1        if self._model_manager.model_storage.is_built_in_or_fine_tuned(model_id):
1            return BuiltInStrategy(model)
1        return RegisteredStrategy(model)
1
1    def _run(
1        self,
1        req,
1        data_getter,
1        deserializer,
1        extract_attrs,
1        resp_cls,
1        single_output: bool,
1    ):
1        model_id = req.modelId
1        try:
1            raw = data_getter(req)
1            full_data = deserializer(raw)
1            inference_attrs = extract_attrs(req)
1
1            predict_length = int(inference_attrs.pop("predict_length", 96))
1            if (
1                predict_length
1                > AINodeDescriptor().get_config().get_ain_inference_max_predict_length()
1            ):
1                raise NumericalRangeException(
1                    "output_length",
1                    1,
1                    AINodeDescriptor()
1                    .get_config()
1                    .get_ain_inference_max_predict_length(),
1                    predict_length,
1                )
1
1            if self._pool_controller.has_request_pools(model_id):
1                # use request pool to accelerate inference when the model instance is already loaded.
1                # TODO: TSBlock -> Tensor codes should be unified
1                data = full_data[1][0]
1                if data.dtype.byteorder not in ("=", "|"):
1                    np_data = data.byteswap()
1                    data = np_data.view(np_data.dtype.newbyteorder())
1                # the inputs should be on CPU before passing to the inference request
1                inputs = torch.tensor(data).unsqueeze(0).float().to("cpu")
1                if model_id == "sundial":
1                    inference_pipeline = TimerSundialInferencePipeline(SundialConfig())
1                elif model_id == "timer_xl":
1                    inference_pipeline = TimerXLInferencePipeline(TimerConfig())
1                else:
1                    raise InferenceModelInternalError(
1                        f"Unsupported model_id: {model_id}"
1                    )
1                infer_req = InferenceRequest(
1                    req_id=generate_req_id(),
1                    model_id=model_id,
1                    inputs=inputs,
1                    inference_pipeline=inference_pipeline,
1                    max_new_tokens=predict_length,
1                )
1                outputs = self._process_request(infer_req)
1                outputs = convert_to_binary(pd.DataFrame(outputs[0]))
1            else:
1                # load model
1                accel = str(inference_attrs.get("acceleration", "")).lower() == "true"
1                model = self._model_manager.load_model(model_id, inference_attrs, accel)
1                # inference by strategy
1                strategy = self._get_strategy(model_id, model)
1                outputs = strategy.infer(
1                    full_data, predict_length=predict_length, **inference_attrs
1                )
1
1            # construct response
1            status = get_status(TSStatusCode.SUCCESS_STATUS)
1
1            if isinstance(outputs, list):
1                return resp_cls(status, outputs[0] if single_output else outputs)
1            return resp_cls(status, outputs if single_output else [outputs])
1
1        except Exception as e:
1            logger.error(e)
1            status = get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
1            empty = b"" if single_output else []
1            return resp_cls(status, empty)
1
1    def forecast(self, req: TForecastReq):
1        return self._run(
1            req,
1            data_getter=lambda r: r.inputData,
1            deserializer=deserialize,
1            extract_attrs=lambda r: {
1                "predict_length": r.outputLength,
1                **(r.options or {}),
1            },
1            resp_cls=TForecastResp,
1            single_output=True,
1        )
1
1    def inference(self, req: TInferenceReq):
1        return self._run(
1            req,
1            data_getter=lambda r: r.dataset,
1            deserializer=deserialize,
1            extract_attrs=lambda r: {
1                "window_interval": getattr(r.windowParams, "windowInterval", None),
1                "window_step": getattr(r.windowParams, "windowStep", None),
1                **(r.inferenceAttributes or {}),
1            },
1            resp_cls=TInferenceResp,
1            single_output=False,
1        )
1
1    def shutdown(self):
1        self._stop_event.set()
1        self._pool_controller.shutdown()
1        while not self._result_queue.empty():
1            self._result_queue.get_nowait()
1        self._result_queue.close()
1