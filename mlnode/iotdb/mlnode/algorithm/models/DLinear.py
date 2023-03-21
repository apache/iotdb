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


import torch
import torch.nn as nn
from iotdb.mlnode.algorithm.layers.decomp_layer import SeriesDecomp
from iotdb.mlnode.exception import BadConfigError


__all__ = ['DLinear', 'dlinear', 'dlinear_individual']

"""
Specific configs for DLinear with default values
"""


def _model_cfg(**kwargs):
    return {
        'individual': False,
        'kernel_size': 25,
        **kwargs
    }


"""
Specific configs for DLinear variants
"""
support_model_cfgs = {
    'dlinear': _model_cfg(
        individual=False),
    'dlinear_individual': _model_cfg(
        individual=True)
}


class DLinear(nn.Module):
    """
    Decomposition Linear
    """
    support_model_names = support_model_cfgs.keys()

    def __init__(
            self,
            individual=False,
            kernel_size=25,
            input_len=96,
            pred_len=96,
            input_vars=1,
            output_vars=1,
            task_type='m',  # TODO, support ms
            model_name='dlinear',
    ):
        super(DLinear, self).__init__()
        self.input_len = input_len
        self.pred_len = pred_len
        self.kernel_size = kernel_size
        self.individual = individual
        self.channels = input_vars
        self.task_type = task_type
        self.model_name = model_name

        # decomposition Kernel Size
        self.decomposition = SeriesDecomp(kernel_size)

        if self.individual:
            self.Linear_Seasonal = nn.ModuleList(
                [nn.Linear(self.input_len, self.pred_len) for _ in range(self.channels)]
            )
            self.Linear_Trend = nn.ModuleList(
                [nn.Linear(self.input_len, self.pred_len) for _ in range(self.channels)]
            )
        else:
            self.Linear_Seasonal = nn.Linear(self.input_len, self.pred_len)
            self.Linear_Trend = nn.Linear(self.input_len, self.pred_len)

    def forward(self, x, *args):
        # x: [Batch, Input length, Channel]
        seasonal_init, trend_init = self.decomposition(x)
        seasonal_init, trend_init = seasonal_init.permute(0, 2, 1), trend_init.permute(0, 2, 1)
        if self.individual:
            seasonal_output = torch.zeros([seasonal_init.size(0), seasonal_init.size(1), self.pred_len],
                                          dtype=seasonal_init.dtype).to(seasonal_init.device)
            trend_output = torch.zeros([trend_init.size(0), trend_init.size(1), self.pred_len],
                                       dtype=trend_init.dtype).to(trend_init.device)
            for i, linear_season_layer in enumerate(self.Linear_Seasonal):
                seasonal_output[:, i, :] = linear_season_layer(seasonal_init[:, i, :])
            for i, linear_trend_layer in enumerate(self.Linear_Trend):
                trend_output[:, i, :] = linear_trend_layer(trend_init[:, i, :])
        else:
            seasonal_output = self.Linear_Seasonal(seasonal_init)
            trend_output = self.Linear_Trend(trend_init)

        x = seasonal_output + trend_output
        return x.permute(0, 2, 1)  # to [Batch, Output length, Channel]


def dlinear(common_config, kernel_size=25, **kwargs):
    cfg = support_model_cfgs['dlinear']
    cfg.update(**common_config)
    if not kernel_size > 0:
        raise BadConfigError('Kernel size of dlinear should larger than 0')
    cfg['kernel_size'] = kernel_size
    return DLinear(**cfg), cfg


def dlinear_individual(common_config, kernel_size=25, **kwargs):
    cfg = support_model_cfgs['dlinear_individual']
    cfg.update(**common_config)
    if not kernel_size > 0:
        raise BadConfigError('Kernel size of dlinear_individual should larger than 0')
    cfg['kernel_size'] = kernel_size
    return DLinear(**cfg), cfg
