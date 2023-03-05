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


from iotdb.mlnode.datats.offline.dataset import *


support_forecasting_dataset = {
    'timeseries': TimeSeriesDataset,
    'window': WindowDataset
}


def _dataset_cfg(**kwargs):
    return {
        'time_embed': 'h',
        **kwargs
    }


support_dataset_cfgs = {
    'timeseries': _dataset_cfg(),

    'window': _dataset_cfg(
        input_len=96,
        pred_len=96,
    )
}


def create_forecasting_dataset(
        dataset_type,
        data_source=None,
        **kwargs,
):
    """ 
    Factory method for all support dataset
    currently implement WindowDataset, TimeSeriesDataset
    for specific dataset configs, see _dataset_cfg in `algorithm/models/MODELNAME.py`

    Args:
        dataset_type: available choice in ['window', 'timeseries']
        data_source: offline multi-variate time series for a while (all pre-fetched in memory)
        kwargs: for specific dataset configs, see returned `dataset_config` with kwargs=None
    Returns:
        dataset: torch.nn.Module
        dataset_config: dict of dataset configurations
    """

    if dataset_type not in support_dataset_cfgs.keys():
        raise RuntimeError(f'Unknown dataset type: ({dataset_type}),'
                           f' which should be one of {support_forecasting_dataset.keys()}')
    assert data_source, 'Data source should be provided'

    dataset_cfg = support_dataset_cfgs[dataset_type]
    dataset_cls = support_forecasting_dataset[dataset_type]

    for k, v in kwargs.items():
        if k in dataset_cfg.keys():
            dataset_cfg[k] = v
    dataset = dataset_cls(data_source, **dataset_cfg)

    return dataset, dataset_cfg
