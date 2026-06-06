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

import sys
import types

import torch

model_loader_stub = types.ModuleType("iotdb.ainode.core.model.model_loader")
model_loader_stub.load_model = lambda *args, **kwargs: None
sys.modules["iotdb.ainode.core.model.model_loader"] = model_loader_stub

from iotdb.ainode.core.inference.pipeline.basic_pipeline import ForecastPipeline


class _NoopForecastPipeline(ForecastPipeline):
    def __init__(self):
        pass

    def forecast(self, inputs, **infer_kwargs):
        return inputs


def test_auto_adapt_padding_defaults_to_zero():
    inputs = [
        {
            "targets": torch.tensor([[1.0, 2.0, 3.0]]),
            "past_covariates": {"cov": torch.tensor([2.0])},
            "future_covariates": {"cov": torch.tensor([4.0])},
        }
    ]

    processed = _NoopForecastPipeline().preprocess(
        inputs, output_length=3, auto_adapt=True
    )

    assert torch.equal(
        processed[0]["past_covariates"]["cov"], torch.tensor([0.0, 0.0, 2.0])
    )
    assert torch.equal(
        processed[0]["future_covariates"]["cov"], torch.tensor([4.0, 0.0, 0.0])
    )


def test_auto_adapt_padding_can_use_nan():
    inputs = [
        {
            "targets": torch.tensor([[1.0, 2.0, 3.0]]),
            "past_covariates": {"cov": torch.tensor([2])},
            "future_covariates": {"cov": torch.tensor([4])},
        }
    ]

    processed = _NoopForecastPipeline().preprocess(
        inputs,
        output_length=3,
        auto_adapt=True,
        auto_adapt_fill_value="NaN",
    )

    assert torch.isnan(processed[0]["past_covariates"]["cov"][:2]).all()
    assert processed[0]["past_covariates"]["cov"][2].item() == 2
    assert processed[0]["past_covariates"]["cov"].dtype == torch.float32
    assert processed[0]["future_covariates"]["cov"][0].item() == 4
    assert torch.isnan(processed[0]["future_covariates"]["cov"][1:]).all()
    assert processed[0]["future_covariates"]["cov"].dtype == torch.float32


def test_auto_adapt_fill_value_rejects_invalid_value():
    inputs = [{"targets": torch.tensor([[1.0, 2.0, 3.0]])}]

    try:
        _NoopForecastPipeline().preprocess(
            inputs,
            output_length=3,
            auto_adapt=True,
            auto_adapt_fill_value="1",
        )
        assert False
    except ValueError as e:
        assert "Unsupported auto_adapt_fill_value" in str(e)


def test_iotdb_data_fetcher_converts_covariates_to_float_tensors():
    session_stub = types.ModuleType("iotdb.Session")
    session_stub.Session = object
    sys.modules["iotdb.Session"] = session_stub

    table_session_stub = types.ModuleType("iotdb.table_session")
    table_session_stub.TableSession = object
    table_session_stub.TableSessionConfig = object
    sys.modules["iotdb.table_session"] = table_session_stub

    field_stub = types.ModuleType("iotdb.utils.Field")
    field_stub.Field = object
    sys.modules["iotdb.utils.Field"] = field_stub

    constants_stub = types.ModuleType("iotdb.utils.IoTDBConstants")
    constants_stub.TSDataType = types.SimpleNamespace(
        INT32=1, INT64=2, FLOAT=3, DOUBLE=4, TIMESTAMP=5, TEXT=6
    )
    sys.modules["iotdb.utils.IoTDBConstants"] = constants_stub

    row_record_stub = types.ModuleType("iotdb.utils.RowRecord")
    row_record_stub.RowRecord = object
    sys.modules["iotdb.utils.RowRecord"] = row_record_stub

    from timecho.ainode.core.ingress.data_fetcher import IoTDBDataFetcher

    series_map = {("__DEFAULT_TAG__",): {"cov": [1, 2, 3]}}
    timestamps_map = {("__DEFAULT_TAG__",): [3, 1, 2]}

    sorted_series_map, sorted_timestamps_map = IoTDBDataFetcher._sort_data_by_timestamp(
        None, series_map, timestamps_map
    )

    covariates = sorted_series_map[("__DEFAULT_TAG__",)]["cov"]
    assert covariates.dtype == torch.float32
    assert torch.equal(covariates, torch.tensor([2.0, 3.0, 1.0]))
    assert sorted_timestamps_map[("__DEFAULT_TAG__",)] == [1, 2, 3]
