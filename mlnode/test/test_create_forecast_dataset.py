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
import os

import requests

from iotdb.mlnode.data_access.enums import DatasetType, DataSourceType
from iotdb.mlnode.data_access.factory import create_forecast_dataset


def test_create_dataset():
    if not os.path.exists('sample_data.csv'):
        response = requests.get('https://cloud.tsinghua.edu.cn/f/9127c193e7254baeaed2/?dl=1')
        with open('sample_data.csv', 'wb') as f:
            f.write(response.content)
    data, data_config = create_forecast_dataset(dataset_type='window',
                                                input_len=192,
                                                pred_len=96,
                                                source_type='file',
                                                filename='sample_data.csv')
    assert data_config['dataset_type'] == str(DatasetType.WINDOW)
    assert data_config['source_type'] == str(DataSourceType.FILE)
    assert data_config['input_vars'] == data.get_variable_num()
    assert data_config['output_vars'] == data.get_variable_num()

    data_item = data[0]
    x, y, x_enc, y_enc = data_item
    assert x.shape[0] == 192
    assert y.shape[0] == 96
    assert x.shape[1] == data.get_variable_num()
    assert y.shape[1] == data.get_variable_num()

    data, data_config = create_forecast_dataset(dataset_type='window',
                                                input_len=192,
                                                pred_len=96,
                                                source_type='file',
                                                filename='sample_data.csv',
                                                query_filter='0,-1',
                                                query_expressions=['root.eg.etth1.*'])
    # config about thrift source not belongs to file source
    assert 'query_expression' not in data_config
    assert 'query_filter' not in data_config


def test_bad_config_dataset1():
    try:
        data, data_config = create_forecast_dataset(dataset_type='dummy_dataset',
                                                    source_type='file')
    except Exception as e:
        print(e)  # ('dataset_type', 'dummy_dataset')
    try:
        data, data_config = create_forecast_dataset(dataset_type='window',
                                                    source_type='dummy_source')
    except Exception as e:
        print(e)  # ('source_type', 'dummy_source')


def test_missing_config_dataset1():
    try:
        data, data_config = create_forecast_dataset(dataset_type='window',
                                                    source_type='file')
    except Exception as e:
        print(e)  # (filename) is Missing


def test_bad_config_dataset2():
    try:
        data, data_config = create_forecast_dataset(dataset_type='window',
                                                    source_type='file',
                                                    filename='sample_data.csv',
                                                    input_vars=1)
    except Exception as e:
        print(e)  # ('input_vars', 1)
