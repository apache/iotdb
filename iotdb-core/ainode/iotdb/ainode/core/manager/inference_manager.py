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
from typing import Dict

import torch
import torch.multiprocessing as mp

from iotdb.ainode.core.config import AINodeDescriptor
from iotdb.ainode.core.constant import TSStatusCode
from iotdb.ainode.core.exception import (
    InferenceModelInternalException,
    NumericalRangeException,
)
from iotdb.ainode.core.inference.inference_request import (
    InferenceRequest,
    InferenceRequestProxy,
)
from iotdb.ainode.core.inference.pipeline.basic_pipeline import (
    ChatPipeline,
    ClassificationPipeline,
    ForecastPipeline,
)
from iotdb.ainode.core.inference.pipeline.pipeline_loader import load_pipeline
from iotdb.ainode.core.inference.pool_controller import PoolController
from iotdb.ainode.core.inference.utils import generate_req_id
from iotdb.ainode.core.log import Logger
from iotdb.ainode.core.manager.model_manager import ModelManager
from iotdb.ainode.core.rpc.status import get_status
from iotdb.ainode.core.util.gpu_mapping import get_available_devices
from iotdb.ainode.core.util.serde import (
    convert_tensor_to_tsblock,
    convert_tsblock_to_tensor,
)
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

logger = Logger()


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
            raise InferenceModelInternalException(str(e))
        finally:
            with self._result_wrapper_lock:
                del self._result_wrapper_map[req_id]

    def _run(
        self,
        req,
        data_getter,
        extract_attrs,
        resp_cls,
        single_batch: bool,
    ):
        model_id = req.modelId
        try:
            raw = data_getter(req)
            inputs = convert_tsblock_to_tensor(raw)

            inference_attrs = extract_attrs(req)
            output_length = int(inference_attrs.pop("output_length", 96))

            # model_inputs_list: Each element is a dict, which contains the following keys:
            #   `targets`: The input tensor for the target variable(s), whose shape is [target_count, input_length].
            model_inputs_list: list[
                dict[str, torch.Tensor | dict[str, torch.Tensor]]
            ] = [{"targets": inputs[0]}]

            if (
                output_length
                > AINodeDescriptor().get_config().get_ain_inference_max_output_length()
            ):
                raise NumericalRangeException(
                    "output_length",
                    1,
                    AINodeDescriptor()
                    .get_config()
                    .get_ain_inference_max_output_length(),
                    output_length,
                )

            if self._pool_controller.has_request_pools(model_id):
                infer_req = InferenceRequest(
                    req_id=generate_req_id(),
                    model_id=model_id,
                    inputs=torch.stack(
                        [data["targets"] for data in model_inputs_list], dim=0
                    ),
                    output_length=output_length,
                )
                outputs = self._process_request(infer_req)
            else:
                model_info = self._model_manager.get_model_info(model_id)
                inference_pipeline = load_pipeline(model_info, device="cpu")
                inputs = inference_pipeline.preprocess(
                    model_inputs_list, output_length=output_length
                )
                if isinstance(inference_pipeline, ForecastPipeline):
                    outputs = inference_pipeline.forecast(
                        inputs, output_length=output_length, **inference_attrs
                    )
                elif isinstance(inference_pipeline, ClassificationPipeline):
                    outputs = inference_pipeline.classify(inputs)
                elif isinstance(inference_pipeline, ChatPipeline):
                    outputs = inference_pipeline.chat(inputs)
                else:
                    outputs = None
                    logger.error("[Inference] Unsupported pipeline type.")
                outputs = inference_pipeline.postprocess(outputs)

            # convert tensor into tsblock for the output in each batch
            output_list = []
            for batch_idx, output in enumerate(outputs):
                output = convert_tensor_to_tsblock(output)
                output_list.append(output)

            return resp_cls(
                get_status(TSStatusCode.SUCCESS_STATUS),
                output_list[0] if single_batch else output_list,
            )

        except Exception as e:
            logger.error(e)
            status = get_status(TSStatusCode.AINODE_INTERNAL_ERROR, str(e))
            empty = b"" if single_batch else []
            return resp_cls(status, empty)

    def forecast(self, req: TForecastReq):
        return self._run(
            req,
            data_getter=lambda r: r.inputData,
            extract_attrs=lambda r: {
                "output_length": r.outputLength,
                **(r.options or {}),
            },
            resp_cls=TForecastResp,
            single_batch=True,
        )

    def inference(self, req: TInferenceReq):
        return self._run(
            req,
            data_getter=lambda r: r.dataset,
            extract_attrs=lambda r: {
                "output_length": int(r.inferenceAttributes.pop("outputLength", 96)),
                **(r.inferenceAttributes or {}),
            },
            resp_cls=TInferenceResp,
            single_batch=False,
        )

    def stop(self):
        self._stop_event.set()
        self._pool_controller.stop()
        while not self._result_queue.empty():
            self._result_queue.get_nowait()
        self._result_queue.close()
