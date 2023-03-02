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
from ..utils.timefeatures import time_features
from ..data_source import DataSource

# for multivariate forecasting only


__all__ = ['TimeSeriesDataset', 'WindowDataset']



class TimeSeriesDataset(Dataset):
    """
    Build Row-by-Row dataset (with each element as multivariable time series at the same time and correponding timestamp embedding)
    
    Args:
        data_source: the whole multivariate time series for a while
        time_embed: embedding frequency, see `utils/timefeatures.py` for more detail

    Returns:
        Random accessable dataset
    """
    def __init__(self, data_source: DataSource, time_embed='h', **kwargs):  
        self.time_embed = time_embed
        self.data = data_source.data
        self.data_stamp = data_source.data_stamp
        self.data_stamp = time_features(data_source.data_stamp, time_embed=self.time_embed).transpose(1, 0)
        self.n_vars = self.data.shape[-1]

    def get_variable_num(self):
        return self.n_vars # number of series in data_source 

    def __getitem__(self, index):
        seq = self.data[index]
        seq_t = self.data_stamp[index]
        return seq, seq_t

    def __len__(self):
        return len(self.data)


class WindowDataset(TimeSeriesDataset):
    """
    Build Windowed dataset (with each element as multivariable time series with a sliding window and correponding timestamps embedding),
    the sliding step is one unit in give data source

    Args:
        data_source: the whole multivariate time series for a while
        time_embed: embedding frequency, see `utils/timefeatures.py` for more detail
        input_len: input window size (unit) [1, 2, ... I]
        pred_len: output window size (unit) right after the input window [I+1, I+2, ... I+P]

    Returns:
        Random accessable dataset
    """
    def __init__(self, data_source, input_len=96, pred_len=96, time_embed='h', **kwargs):
        self.time_embed = time_embed
        self.input_len = input_len
        self.pred_len = pred_len
        self.data = data_source.data
        self.data_stamp = time_features(data_source.data_stamp, time_embed=self.time_embed).transpose(1, 0)
        self.n_vars = self.data.shape[-1]

    def __getitem__(self, index):
        s_begin = index
        s_end = s_begin + self.input_len
        r_begin = s_end
        r_end = s_end + self.pred_len
        seq_x = self.data[s_begin:s_end]
        seq_y = self.data[r_begin:r_end]
        seq_x_t = self.data_stamp[s_begin:s_end]
        seq_y_t = self.data_stamp[r_begin:r_end]
        return seq_x, seq_y, seq_x_t, seq_y_t

    def __len__(self):
        return len(self.data) - self.input_len - self.pred_len + 1


#TODO: should be more simple, numpy array maybe
class InferenceDataset(Dataset):
    pass