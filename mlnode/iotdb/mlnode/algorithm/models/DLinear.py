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
from .layers.decomp_layer import SeriesDecomp


__all__ = ['DLinear', 'dlinear', 'dlinear_individual']


"""
Specific configs for DLinear
"""
def _model_cfg(**kwargs):
    return {
        'model_name': 'dlinear',
        'individual': False,
        'kernel_size': 25,
        **kwargs
    }


"""
Specific configs for DLinear variants
"""
support_model_cfgs = {
    'dlinear': _model_cfg(
        model_name='dlinear',
        individual=False),
    'dlinear_individual': _model_cfg(
        model_name='dlinear_individual',
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
        task_type='m', # TODO, support ms
        **kwargs
    ):
        super(DLinear, self).__init__()
        self.input_len = input_len
        self.pred_len = pred_len
        self.kernel_size = kernel_size
        self.individual = individual
        self.channels = input_vars

        # decomposition Kernel Size
        self.decomposition = SeriesDecomp(kernel_size)

        if self.individual:
            self.Linear_Seasonal = nn.ModuleList()
            self.Linear_Trend = nn.ModuleList()

            for _ in range(self.channels):
                self.Linear_Seasonal.append(nn.Linear(self.input_len,self.pred_len))
                self.Linear_Trend.append(nn.Linear(self.input_len,self.pred_len))
        else:
            self.Linear_Seasonal = nn.Linear(self.input_len,self.pred_len)
            self.Linear_Trend = nn.Linear(self.input_len,self.pred_len)


    def forward(self, x, x_t, y, y_t):
        # x: [Batch, Input length, Channel]
        seasonal_init, trend_init = self.decomposition(x)
        seasonal_init, trend_init = seasonal_init.permute(0,2,1), trend_init.permute(0,2,1)
        if self.individual:
            seasonal_output = torch.zeros([seasonal_init.size(0),seasonal_init.size(1),self.pred_len],dtype=seasonal_init.dtype).to(seasonal_init.device)
            trend_output = torch.zeros([trend_init.size(0),trend_init.size(1),self.pred_len],dtype=trend_init.dtype).to(trend_init.device)
            for i in range(self.channels):
                seasonal_output[:,i,:] = self.Linear_Seasonal[i](seasonal_init[:,i,:])
                trend_output[:,i,:] = self.Linear_Trend[i](trend_init[:,i,:])
        else:
            seasonal_output = self.Linear_Seasonal(seasonal_init)
            trend_output = self.Linear_Trend(trend_init)

        x = seasonal_output + trend_output
        return x.permute(0,2,1) # to [Batch, Output length, Channel]


def dlinear(kernel_size=25, **kwargs):
    cfg = support_model_cfgs['dlinear']
    cfg['kernel_size']=kernel_size
    cfg.update(**kwargs)
    return DLinear(**cfg), cfg


def dlinear_individual(kernel_size=25, **kwargs):
    cfg = support_model_cfgs['dlinear_individual']
    cfg['kernel_size']=kernel_size
    cfg.update(**kwargs)
    return DLinear(**cfg), cfg

