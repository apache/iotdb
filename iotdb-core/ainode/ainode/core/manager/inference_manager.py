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
from abc import ABC, abstractmethod

import pandas as pd
import torch
from iotdb.tsfile.utils.tsblock_serde import deserialize

from ainode.core.constant import TSStatusCode
from ainode.core.exception import (
    InferenceModelInternalError,
    InvalidWindowArgumentError,
    runtime_error_extractor,
)
from ainode.core.log import Logger
from ainode.core.manager.model_manager import ModelManager
from ainode.core.model.sundial.modeling_sundial import SundialForPrediction
from ainode.core.model.timerxl.modeling_timer import TimerForPrediction
from ainode.core.util.serde import convert_to_binary
from ainode.core.util.status import get_status
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
    def infer(self, full_data, window_interval=None, window_step=None, **kwargs):
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


def _get_strategy(model_id, model):
    if isinstance(model, TimerForPrediction):
        return TimerXLStrategy(model)
    if isinstance(model, SundialForPrediction):
        return SundialStrategy(model)
    if model_id.startswith("_"):
        return BuiltInStrategy(model)
    return RegisteredStrategy(model)


class InferenceManager:
    def __init__(self, model_manager: ModelManager):
        self.model_manager = model_manager

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
        logger.info(f"Start processing for {model_id}")
        try:
            raw = data_getter(req)
            full_data = deserializer(raw)
            inference_attrs = extract_attrs(req)

            # load model
            accel = str(inference_attrs.get("acceleration", "")).lower() == "true"
            model = self.model_manager.load_model(model_id, accel)

            # inference by strategy
            strategy = _get_strategy(model_id, model)
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
