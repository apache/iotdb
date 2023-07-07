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
from typing import Tuple

import numpy as np
from torch.utils.data import Dataset

from iotdb.mlnode.data_access.offline.source import DataSource
from iotdb.mlnode.data_access.utils.timefeatures import time_features


class TimeSeriesDataset(Dataset):
    """
    Build Row-by-Row dataset (with each element as multivariable time series at
    the same time and corresponding timestamp embedding)

    Args:
        data_source: the whole multivariate time series for a while
        time_embed: embedding frequency, see `utils/timefeatures.py` for more detail

    Returns:
        Random accessible dataset
    """

    def __init__(self, data_source: DataSource, time_embed: str = 'h'):
        self.time_embed = time_embed
        self.data = data_source.get_data()
        self.data_stamp = time_features(data_source.get_timestamp(), time_embed=self.time_embed).transpose(1, 0)
        self.n_vars = self.data.shape[-1]

    def get_variable_num(self) -> int:
        return self.n_vars  # number of series in data_source

    def __getitem__(self, index) -> Tuple[np.ndarray, np.ndarray]:
        seq = self.data[index]
        seq_t = self.data_stamp[index]
        return seq, seq_t

    def __len__(self) -> int:
        return len(self.data)


class WindowDataset(Dataset):
    """
    Build Windowed dataset (with each element as multivariable time series
    with a sliding window and corresponding timestamps embedding),
    the sliding step is one unit in give data source

    Args:
        data_source: the whole multivariate time series for a while
        time_embed: embedding frequency, see `utils/timefeatures.py` for more detail
        input_len: input window size (unit) [1, 2, ... I]
        pred_len: output window size (unit) right after the input window [I+1, I+2, ... I+P]

    Returns:
        Random accessible dataset
    """

    def __init__(self,
                 data_source: DataSource = None,
                 input_len: int = 96,
                 pred_len: int = 96,
                 time_embed: str = 'h'):
        self.input_len = input_len
        self.pred_len = pred_len
        self.time_embed = time_embed
        self.data = data_source.get_data()
        self.data_stamp = time_features(data_source.get_timestamp(), time_embed=self.time_embed).transpose(1, 0)
        self.n_vars = self.data.shape[-1]
        if input_len > self.data.shape[0]:
            raise RuntimeError('input_len should not be larger than the number of time series points')
        if pred_len > self.data.shape[0]:
            raise RuntimeError('pred_len should not be larger than the number of time series points')

    def __getitem__(self, index) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        s_begin = index
        s_end = s_begin + self.input_len
        r_begin = s_end
        r_end = s_end + self.pred_len
        seq_x = self.data[s_begin:s_end]
        seq_y = self.data[r_begin:r_end]
        seq_x_t = self.data_stamp[s_begin:s_end]
        seq_y_t = self.data_stamp[r_begin:r_end]
        return seq_x, seq_y, seq_x_t, seq_y_t

    def __len__(self) -> int:
        return len(self.data) - self.input_len - self.pred_len + 1

    def get_variable_num(self) -> int:
        return self.n_vars  # number of series in data_source

