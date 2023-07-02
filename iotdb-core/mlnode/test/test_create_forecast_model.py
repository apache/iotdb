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

from iotdb.mlnode.algorithm.enums import ForecastTaskType
from iotdb.mlnode.algorithm.factory import create_forecast_model
from iotdb.mlnode.exception import BadConfigValueError


def test_create_forecast_model():
    model, model_config = create_forecast_model(model_name='dlinear',
                                                kernel_size=25, input_vars=8, output_vars=8)
    sample_input = torch.randn(1, model_config['input_len'], model_config['input_vars'])
    output = model(sample_input)
    assert output.shape[1] == model_config['pred_len']
    assert output.shape[2] == model_config['output_vars']
    assert model_config['kernel_size'] == 25

    model, model_config = create_forecast_model(model_name='nbeats',
                                                kernel_size=25, d_model=64)
    assert model_config['d_model'] == 64
    assert 'kernel_size' not in model_config  # config kernel_size not belongs to nbeats model


def test_bad_config_model1():
    try:
        model, models = create_forecast_model(model_name='dlinear_dummy',
                                              kernel_size=25, input_vars=8, output_vars=8)
    except BadConfigValueError as e:
        print(e)  # BadConfigValueError: ('model_name', 'dlinear_dummy')


def test_bad_config_model2():
    try:
        model, models = create_forecast_model(model_name='dlinear',
                                              kernel_size=25, input_vars=0)
    except BadConfigValueError as e:
        print(e)  # ('input_vars', 0)


def test_bad_config_model3():
    try:
        model, models = create_forecast_model(model_name='dlinear',
                                              kernel_size=-1, input_vars=8, output_vars=8)
    except BadConfigValueError as e:
        print(e)  # ('kernel_size', -1)


def test_bad_config_model4():
    try:
        model, models = create_forecast_model(model_name='dlinear',
                                              forecast_task_type='dummy_task')
    except BadConfigValueError as e:
        print(e)  # ('forecast_task_type', 'dummy_task')


def test_bad_config_model5():
    try:
        model, models = create_forecast_model(model_name='dlinear',
                                              forecast_task_type=ForecastTaskType.ENDOGENOUS,
                                              kernel_size=25, input_vars=8, output_vars=1)
    except BadConfigValueError as e:
        print(e)  # ('forecast_task_type', <ForecastTaskType.ENDOGENOUS: 'endogenous'>)
