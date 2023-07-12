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

import math

import torch
import torch.nn as nn


class MovingAverageBlock(nn.Module):
    """ Moving average block to highlight the trend of time series """

    def __init__(self, kernel_size, stride):
        super(MovingAverageBlock, self).__init__()
        self.kernel_size = kernel_size
        self.avg = nn.AvgPool1d(kernel_size=kernel_size, stride=stride, padding=0)

    def forward(self, x):
        # padding on the both ends of time series
        front = x[:, 0:1, :].repeat(1, self.kernel_size - 1 - math.floor((self.kernel_size - 1) // 2), 1)
        end = x[:, -1:, :].repeat(1, math.floor((self.kernel_size - 1) // 2), 1)
        x = torch.cat([front, x, end], dim=1)
        x = self.avg(x.permute(0, 2, 1))
        x = x.permute(0, 2, 1)
        return x


class SeriesDecompositionBlock(nn.Module):
    """ Series decomposition block """

    def __init__(self, kernel_size):
        super(SeriesDecompositionBlock, self).__init__()
        self.moving_avg = MovingAverageBlock(kernel_size, stride=1)

    def forward(self, x):
        moving_mean = self.moving_avg(x)
        res = x - moving_mean
        return res, moving_mean


class DLinear(nn.Module):
    """ Decomposition Linear Model """

    def __init__(
            self,
            kernel_size: int,
            input_len: int,
            pred_len: int,
            input_vars: int,
            # TODO : forecast_task_type=ForecastTaskType.ENDOGENOUS
    ):
        super(DLinear, self).__init__()
        self.input_len = input_len
        self.pred_len = pred_len
        self.kernel_size = kernel_size
        self.channels = input_vars

        # decomposition Kernel Size
        self.decomposition = SeriesDecompositionBlock(kernel_size)
        self.linear_seasonal = nn.Linear(self.input_len, self.pred_len)
        self.linear_trend = nn.Linear(self.input_len, self.pred_len)

    def forward(self, x, *args):
        # x: [Batch, Input length, Channel]
        seasonal_init, trend_init = self.decomposition(x)
        seasonal_init, trend_init = seasonal_init.permute(0, 2, 1), trend_init.permute(0, 2, 1)

        seasonal_output = self.linear_seasonal(seasonal_init)
        trend_output = self.linear_trend(trend_init)

        x = seasonal_output + trend_output
        return x.permute(0, 2, 1)  # to [Batch, Output length, Channel]


class DLinearIndividual(nn.Module):
    """ Decomposition Linear Model (individual) """

    def __init__(
            self,
            kernel_size=25,
            input_len=96,
            pred_len=96,
            input_vars=1,
            # TODO : forecast_task_type=ForecastTaskType.ENDOGENOUS
    ):
        super(DLinearIndividual, self).__init__()
        self.input_len = input_len
        self.pred_len = pred_len
        self.kernel_size = kernel_size
        self.channels = input_vars

        self.decomposition = SeriesDecompositionBlock(kernel_size)
        self.Linear_Seasonal = nn.ModuleList(
            [nn.Linear(self.input_len, self.pred_len) for _ in range(self.channels)]
        )
        self.Linear_Trend = nn.ModuleList(
            [nn.Linear(self.input_len, self.pred_len) for _ in range(self.channels)]
        )

    def forward(self, x, *args):
        # x: [Batch, Input length, Channel]
        seasonal_init, trend_init = self.decomposition(x)
        seasonal_init, trend_init = seasonal_init.permute(0, 2, 1), trend_init.permute(0, 2, 1)

        seasonal_output = torch.zeros([seasonal_init.size(0), seasonal_init.size(1), self.pred_len],
                                      dtype=seasonal_init.dtype).to(seasonal_init.device)
        trend_output = torch.zeros([trend_init.size(0), trend_init.size(1), self.pred_len],
                                   dtype=trend_init.dtype).to(trend_init.device)
        for i, linear_season_layer in enumerate(self.Linear_Seasonal):
            seasonal_output[:, i, :] = linear_season_layer(seasonal_init[:, i, :])
        for i, linear_trend_layer in enumerate(self.Linear_Trend):
            trend_output[:, i, :] = linear_trend_layer(trend_init[:, i, :])

        x = seasonal_output + trend_output
        return x.permute(0, 2, 1)  # to [Batch, Output length, Channel]



