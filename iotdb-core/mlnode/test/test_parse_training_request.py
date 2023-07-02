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
from iotdb.mlnode.exception import MissingConfigError, WrongTypeConfigError
from iotdb.mlnode.parser import parse_training_request
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq


def test_parse_training_request():
    model_id = 'mid_etth1_dlinear_default'
    is_auto = False
    model_configs = {
        'task_class': 'forecast_training_task',
        'source_type': 'thrift',
        'dataset_type': 'window',  # or use DatasetType.WINDOW,
        'filename': 'ETTh1.csv',
        'time_embed': 'h',
        'input_len': 96,
        'pred_len': 96,
        'model_name': 'dlinear',
        'input_vars': 7,
        'output_vars': 7,
        'forecast_type': 'endogenous',
        'kernel_size': 25,
        'learning_rate': 1e-3,
        'batch_size': 32,
        'num_workers': 0,
        'epochs': 10,
        'metric_names': ['MSE', 'MAE']
    }
    query_expressions = ['root.eg.etth1.**', 'root.eg.etth1.**', 'root.eg.etth1.**']
    query_filter = '0,1501516800000'
    req = TCreateTrainingTaskReq(
        modelId=str(model_id),
        isAuto=is_auto,
        modelConfigs={k: str(v) for k, v in model_configs.items()},
        queryExpressions=[str(query) for query in query_expressions],
        queryFilter=str(query_filter),
    )
    data_config, model_config, task_config = parse_training_request(req)
    for config in model_configs:
        if config in data_config:
            assert data_config[config] == model_configs[config]
        if config in model_config:
            assert model_config[config] == model_configs[config]
        if config in task_config:
            assert task_config[config] == model_configs[config]


def test_missing_argument():
    # missing model_name
    model_id = 'mid_etth1_dlinear_default'
    is_auto = False
    model_configs = {
        'task_class': 'forecast_training_task',
        'source_type': 'thrift',
        'dataset_type': 'window',
        'filename': 'ETTh1.csv',
        'time_embed': 'h',
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 7,
        'output_vars': 7,
        'forecast_type': 'endogenous',
        'kernel_size': 25,
        'learning_rate': 1e-3,
        'batch_size': 32,
        'num_workers': 0,
        'epochs': 10,
        'metric_names': ['MSE', 'MAE']
    }
    query_expressions = ['root.eg.etth1.**', 'root.eg.etth1.**', 'root.eg.etth1.**']
    query_filter = '0,1501516800000'
    req = TCreateTrainingTaskReq(
        modelId=str(model_id),
        isAuto=is_auto,
        modelConfigs={k: str(v) for k, v in model_configs.items()},
        queryExpressions=[str(query) for query in query_expressions],
        queryFilter=str(query_filter),
    )
    try:
        parse_training_request(req)
    except Exception as e:
        assert e.message == MissingConfigError(config_name='model_name').message


def test_wrong_argument_type():
    model_id = 'mid_etth1_dlinear_default'
    is_auto = False
    model_configs = {
        'task_class': 'forecast_training_task',
        'source_type': 'thrift',
        'dataset_type': 'window',
        'filename': 'ETTh1.csv',
        'time_embed': 'h',
        'input_len': 96.7,
        'pred_len': 96,
        'model_name': 'dlinear',
        'input_vars': 7,
        'output_vars': 7,
        'forecast_type': 'endogenous',
        'kernel_size': 25,
        'learning_rate': 1e-3,
        'batch_size': 32,
        'num_workers': 0,
        'epochs': 10,
        'metric_names': ['MSE', 'MAE']
    }
    query_expressions = ['root.eg.etth1.**', 'root.eg.etth1.**', 'root.eg.etth1.**']
    query_filter = '0,1501516800000'
    req = TCreateTrainingTaskReq(
        modelId=str(model_id),
        isAuto=is_auto,
        modelConfigs={k: str(v) for k, v in model_configs.items()},
        queryExpressions=[str(query) for query in query_expressions],
        queryFilter=str(query_filter),
    )
    try:
        data_config, model_config, task_config = parse_training_request(req)
    except Exception as e:
        assert e.message == WrongTypeConfigError(config_name='input_len', expected_type='int').message
