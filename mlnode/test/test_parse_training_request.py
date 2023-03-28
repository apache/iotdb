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

from iotdb.mlnode.util import parse_training_request
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq


def test_parse_training_request():
    modelId = 'mid_etth1_dlinear_default'
    isAuto = False
    modelConfigs = {
        'task_class': 'forecast_training_task',
        'source_type': 'thrift',
        'dataset_type': 'window',
        'filename': 'ETTh1.csv',
        'time_embed': 'h',
        'input_len': 96,
        'pred_len': 96,
        'model_name': 'dlinear',
        'input_vars': 7,
        'output_vars': 7,
        'task_type': 'm',
        'kernel_size': 25,
        'learning_rate': 1e-3,
        'batch_size': 32,
        'num_workers': 0,
        'epochs': 10,
        'metric_names': ['MSE', 'MAE']
    }
    req = TCreateTrainingTaskReq(
        modelId=str(modelId),
        isAuto=isAuto,
        modelConfigs={k: str(v) for k, v in modelConfigs.items()},
    )
    data_conf, model_conf, task_conf = parse_training_request(req)
    for config in modelConfigs:
        if config in data_conf:
            assert data_conf[config] == modelConfigs[config]
        if config in model_conf:
            assert model_conf[config] == modelConfigs[config]
        if config in task_conf:
            assert task_conf[config] == modelConfigs[config]
