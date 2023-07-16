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
import torch

from iotdb.mlnode.algorithm.factory import (ForecastModelType,
                                            create_forecast_model)
from iotdb.mlnode.algorithm.hyperparameter import (HyperparameterName,
                                                   parse_fixed_hyperparameters)
from iotdb.mlnode.constant import OptionsKey
from iotdb.mlnode.exception import (BadConfigValueError, MissingOptionError,
                                    UnsupportedError)
from iotdb.mlnode.parser import ForecastTaskOptions, parse_task_options


def test_create_forecast_model():
    d_forecast_task_options = ForecastTaskOptions({OptionsKey.MODEL_TYPE: ForecastModelType.DLINEAR.value,
                                                   OptionsKey.AUTO_TUNING: False})
    d_configs = {HyperparameterName.KERNEL_SIZE.value: "25", HyperparameterName.USE_GPU.value: "False"}
    model_configs, _ = parse_fixed_hyperparameters(d_forecast_task_options, d_configs)
    model_configs[HyperparameterName.INPUT_VARS.value] = 8
    model_configs[OptionsKey.INPUT_LENGTH.value] = d_forecast_task_options.input_length
    model_configs[OptionsKey.PREDICT_LENGTH.value] = d_forecast_task_options.predict_length
    model = create_forecast_model(d_forecast_task_options, model_configs)
    sample_input = torch.randn(1, model_configs[OptionsKey.INPUT_LENGTH.value],
                               model_configs[HyperparameterName.INPUT_VARS.value])
    output = model(sample_input)
    assert output.shape[1] == model_configs[OptionsKey.PREDICT_LENGTH.value]
    assert output.shape[2] == model_configs[HyperparameterName.INPUT_VARS.value]
    assert model_configs[HyperparameterName.KERNEL_SIZE.value] == 25

    n_forecast_task_options = ForecastTaskOptions({OptionsKey.MODEL_TYPE: ForecastModelType.NBEATS.value})
    n_model_configs = {HyperparameterName.KERNEL_SIZE: 25, HyperparameterName.D_MODEL: 64}
    model, model_config = create_forecast_model(n_forecast_task_options, n_model_configs)
    assert model_config['d_model'] == 64
    assert 'kernel_size' not in model_config  # config kernel_size not belongs to nbeats model


def test_bad_config_model1():
    try:
        d_forecast_task_options = ForecastTaskOptions({OptionsKey.MODEL_TYPE: "dlinear_dummy",
                                                       OptionsKey.AUTO_TUNING: False})
        d_configs = {HyperparameterName.KERNEL_SIZE.value: "25", HyperparameterName.USE_GPU.value: "False"}
        create_forecast_model(d_forecast_task_options, d_configs)
    except UnsupportedError as e:
        assert e.message == "model_type dlinear_dummy is not supported in current version"


def test_bad_config_model2():
    try:
        d_forecast_task_options = ForecastTaskOptions({OptionsKey.MODEL_TYPE: "dlinear",
                                                       OptionsKey.AUTO_TUNING: False})
        d_configs = {HyperparameterName.KERNEL_SIZE.value: "-1", HyperparameterName.USE_GPU.value: "False"}
        parse_fixed_hyperparameters(d_forecast_task_options, d_configs)
    except BadConfigValueError as e:
        assert e.message == "Bad value [-1] for config kernel_size. Expect value between 1 and 10000000000.0, " \
                            "got -1 instead."


def test_bad_config_model3():
    try:
        d_task_options = {OptionsKey.MODEL_TYPE: "dlinear",
                          OptionsKey.AUTO_TUNING: False}
        parse_task_options(d_task_options)
    except MissingOptionError as e:
        assert e.message == "Missing task option: task_type"


def test_bad_config_model5():
    try:
        d_task_options = {OptionsKey.MODEL_TYPE: "dlinear",
                          OptionsKey.TASK_TYPE: "dummy_task",
                          OptionsKey.AUTO_TUNING: False}
        parse_task_options(d_task_options)
    except UnsupportedError as e:
        assert e.message == "task_type dummy_task is not supported in current version"
