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
from torch.utils.data import Dataset

from iotdb.mlnode.data_access.enums import DatasetType
from iotdb.mlnode.data_access.offline.dataset import (TimeSeriesDataset,
                                                      WindowDataset)
from iotdb.mlnode.data_access.offline.source import (FileDataSource,
                                                     ThriftDataSource)
from iotdb.mlnode.exception import BadConfigValueError, MissingConfigError

support_forecasting_dataset = {
    DatasetType.TIMESERIES: TimeSeriesDataset,
    DatasetType.WINDOW: WindowDataset
}


def _dataset_config(**kwargs):
    return {
        'time_embed': 'h',
        **kwargs
    }


support_dataset_configs = {
    DatasetType.TIMESERIES: _dataset_config(),
    DatasetType.WINDOW: _dataset_config(
        input_len=96,
        pred_len=96,
    )
}


def create_forecast_dataset(
        source_type,
        dataset_type,
        **kwargs,
) -> [Dataset, dict]:
    """
    Factory method for all support dataset
    currently implement WindowDataset, TimeSeriesDataset
    for specific dataset configs, see _dataset_config in `algorithm/models/MODELNAME.py`

    Args:
        dataset_type: available choice in support_forecasting_dataset
        source_type:  available choice in ['file', 'thrift']
        kwargs: for specific dataset configs, see returned `dataset_config` with kwargs=None

    Returns:
        dataset: torch.nn.Module
        dataset_config: dict of dataset configurations
    """
    if dataset_type not in support_forecasting_dataset.keys():
        raise BadConfigValueError('dataset_type', dataset_type,
                                  f'It should be one of {list(support_forecasting_dataset.keys())}')

    if source_type == 'file':
        if 'filename' not in kwargs.keys():
            raise MissingConfigError('filename')
        datasource = FileDataSource(kwargs['filename'])
    elif source_type == 'thrift':
        if 'query_expressions' not in kwargs.keys():
            raise MissingConfigError('query_expressions')
        if 'query_filter' not in kwargs.keys():
            raise MissingConfigError('query_filter')
        datasource = ThriftDataSource(kwargs['query_expressions'], kwargs['query_filter'])
    else:
        raise BadConfigValueError('source_type', source_type, "It should be one of ['file', 'thrift]")

    dataset_fn = support_forecasting_dataset[dataset_type]
    dataset_config = support_dataset_configs[dataset_type]

    for k, v in kwargs.items():
        if k in dataset_config.keys():
            dataset_config[k] = v

    dataset = dataset_fn(datasource, **dataset_config)

    if 'input_vars' in kwargs.keys() and dataset.get_variable_num() != kwargs['input_vars']:
        raise BadConfigValueError('input_vars', kwargs['input_vars'],
                                  f'Variable number of fetched data: ({dataset.get_variable_num()})'
                                  f' should be consistent with input_vars')

    data_config = dataset_config.copy()
    data_config['input_vars'] = dataset.get_variable_num()
    data_config['output_vars'] = dataset.get_variable_num()
    data_config['source_type'] = source_type
    data_config['dataset_type'] = dataset_type

    return dataset, data_config
