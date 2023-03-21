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
from iotdb.mlnode.algorithm.model_factory import create_forecast_model


def test_create_model():
    model, model_cfg = create_forecast_model(model_name='dlinear')
    assert model
    assert model_cfg['model_name'] == 'dlinear'

    model, model_cfg = create_forecast_model(
        model_name='nbeats',
        input_len=96,
        pred_len=96,
        input_vars=7,
        output_vars=7,
    )
    sample_input = torch.randn(1, model_cfg['input_len'], model_cfg['input_vars'])
    sample_output = model(sample_input)
    assert model
    assert model_cfg['model_name'] == 'nbeats'
    assert sample_output.shape[1] == model_cfg['pred_len']
    assert sample_output.shape[2] == model_cfg['output_vars']

