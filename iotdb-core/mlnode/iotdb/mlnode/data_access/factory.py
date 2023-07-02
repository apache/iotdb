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
from typing import Dict, Tuple

from torch.utils.data import Dataset

from iotdb.mlnode.constant import TaskType
from iotdb.mlnode.data_access.enums import DatasetType, DataSourceType
from iotdb.mlnode.data_access.offline.dataset import (TimeSeriesDataset,
                                                      WindowDataset)
from iotdb.mlnode.data_access.offline.source import (FileDataSource,
                                                     ThriftDataSource)
from iotdb.mlnode.exception import BadConfigValueError, MissingConfigError
from iotdb.mlnode.parser import TaskOptions


def _dataset_common_config(**kwargs):
    return {
        'time_embed': 'h',
        **kwargs
    }


_dataset_default_config_dict = {
    DatasetType.TIMESERIES: _dataset_common_config(),
    DatasetType.WINDOW: _dataset_common_config(
        input_len=96,
        pred_len=96,
    )
}


def create_dataset(query_body: str, task_options: TaskOptions) -> Dataset:
    task_type = task_options.get_task_type()
    if task_type == TaskType.FORECAST:
        return create_forecast_dataset(query_body, task_options)
    else:
        raise Exception(f"task type {task_type} not supported.")


def create_forecast_dataset(query_body: str, task_options: TaskOptions) -> Dataset:
    """
    Factory method for all support dataset
    currently implement two types of PyTorch dataset: WindowDataset, TimeSeriesDataset
    support two types of offline data source: FileDataSource and ThriftDataSource
    for specific dataset/datasource configs, see _dataset_config in `dataset.py` and `source.py`

    Args:
        dataset_type: see data_access/enums for available choices
        source_type:  see data_access/enums for available cho ices
        kwargs: for specific dataset configs, see returned `dataset_config` with kwargs=None

    Returns:
        dataset: torch.nn.Module
        dataset_config: dict of dataset configurations
    """
    if source_type == DataSourceType.FILE:
        if 'filename' not in kwargs.keys():
            raise MissingConfigError('filename')
        datasource = FileDataSource(kwargs['filename'])
    elif source_type == DataSourceType.THRIFT:
        if 'query_expressions' not in kwargs.keys():
            raise MissingConfigError('query_expressions')
        if 'query_filter' not in kwargs.keys():
            raise MissingConfigError('query_filter')
        datasource = ThriftDataSource(kwargs['query_expressions'], kwargs['query_filter'])
    else:
        raise BadConfigValueError('source_type', source_type, f"It should be one of {list(DataSourceType)}")

    if dataset_type not in list(DatasetType):
        raise BadConfigValueError('dataset_type', dataset_type, f'It should be one of {list(DatasetType)}')
    dataset_config = _dataset_default_config_dict[dataset_type]

    for k, v in kwargs.items():
        if k in dataset_config.keys():
            dataset_config[k] = v

    if dataset_type == DatasetType.TIMESERIES:
        dataset = TimeSeriesDataset(datasource, **dataset_config)
    elif dataset_type == DatasetType.WINDOW:
        dataset = WindowDataset(datasource, **dataset_config)
    else:
        raise BadConfigValueError('dataset_type', dataset_type, f'It should be one of {list(DatasetType)}')

    if 'input_vars' in kwargs.keys() and dataset.get_variable_num() != kwargs['input_vars']:
        raise BadConfigValueError('input_vars', kwargs['input_vars'],
                                  f'Variable number of fetched data should be consistent with '
                                  f'input_vars, but got: {dataset.get_variable_num()}')

    data_config = dataset_config.copy()
    data_config['input_vars'] = dataset.get_variable_num()
    data_config['output_vars'] = dataset.get_variable_num()
    data_config['source_type'] = str(source_type)
    data_config['dataset_type'] = str(dataset_type)

    return dataset, data_config
