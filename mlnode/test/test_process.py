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

from iotdb.mlnode.util import _get_data_config_parser, _get_task_config_parser, _get_model_config_parser
from iotdb.mlnode.client import MLNodeClient

def test_create_training_task():
    pass
#     client = MLNodeClient(host="127.0.0.1", port=10810)
#     config_dlinear = {
#         'task_class': 'forecast_training_task',
#         'source_type': 'thrift',
#         'dataset_type': 'window',
#         'time_embed': 'h',
#         'input_len': 96,
#         'pred_len': 96,
#         'model_name': 'dlinear',
#         'input_vars': 7,
#         'output_vars': 7,
#         'task_type': 'm',
#         'kernel_size': 25,
#         'learning_rate': 1e-3,
#         'batch_size': 32,
#         'num_workers': 0,
#         'epochs': 10,
#         'metric_names': ['MSE', 'MAE']
#     }
#     '''
#     CREATE MODEL mid_etth1_dlinear_default
#     WITH
#         INPUT_LEN=96,
#         OUTPUT_LEN=96
#         MODEL_TYPE=dlinear,
#         BATCH_SIZE=32,
#         LEARNING_RATE=0.001,
#         EPOCHS=10,
#     BEGIN
#         SELECT *
#         FROM root.eg.etth1.**
#         WHERE
#     END
#     '''
#     print(client.create_training_task(
#         model_id='mid_etth1_dlinear_default',  # 'mid_etth1_nbeats_default', #
#         is_auto=False,
#         model_configs=config_dlinear,  # config_nbeats,       #
#         query_expressions=['root.eg.etth1.**'],  # 7 variables
#         query_filter='0,1501516800000',  # timestamp in ms
#     ))

