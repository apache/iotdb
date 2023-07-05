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
from iotdb.mlnode.parser import TaskOptions, ForecastTaskOptions


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
        return create_forecast_dataset(query_body, type(ForecastTaskOptions)(task_options))
    else:
        raise Exception(f"task type {task_type} not supported.")


def create_forecast_dataset(query_body: str, task_options: ForecastTaskOptions) -> Dataset:
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
    datasource = ThriftDataSource(query_body)
    dataset = WindowDataset(datasource, task_options.input_length,task_options.predict_length)

    return dataset
