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

import torch
import torch.nn as nn


class GenericBasis(nn.Module):
    """ Generic basis function """

    def __init__(self, backcast_size: int, forecast_size: int):
        super().__init__()
        self.backcast_size = backcast_size
        self.forecast_size = forecast_size

    def forward(self, theta: torch.Tensor):
        return theta[:, :self.backcast_size], theta[:, -self.forecast_size:]


block_dict = {
    'generic': GenericBasis,
}


class NBeatsBlock(nn.Module):
    """ N-BEATS block which takes a basis function as an argument """

    def __init__(self,
                 input_size,
                 theta_size: int,
                 basis_function: nn.Module,
                 layers: int,
                 layer_size: int):
        """
        N-BEATS block

        Args:
            input_size: input sample size
            theta_size:  number of parameters for the basis function
            basis_function: basis function which takes the parameters and produces backcast and forecast
            layers: number of layers
            layer_size: layer size
        """
        super().__init__()
        self.layers = nn.ModuleList([nn.Linear(in_features=input_size, out_features=layer_size)] + [
            nn.Linear(in_features=layer_size, out_features=layer_size) for _ in range(layers - 1)])
        self.basis_parameters = nn.Linear(in_features=layer_size, out_features=theta_size)
        self.basis_function = basis_function

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        block_input = x
        for layer in self.layers:
            block_input = torch.relu(layer(block_input))
        basis_parameters = self.basis_parameters(block_input)
        return self.basis_function(basis_parameters)


class NBeatsUnivariable(nn.Module):
    """ N-Beats Model (uni-variable) """

    def __init__(self, blocks: nn.ModuleList):
        super().__init__()
        self.blocks = blocks

    def forward(self, x):
        residuals = x
        forecast = None
        for _, block in enumerate(self.blocks):
            backcast, block_forecast = block(residuals)
            residuals = (residuals - backcast)
            if forecast is None:
                forecast = block_forecast
            else:
                forecast += block_forecast
        return forecast


class NBeats(nn.Module):
    """ Neural Basis Expansion Analysis Time Series """

    def __init__(
            self,
            block_type='generic',
            d_model=128,
            inner_layers=4,
            outer_layers=4,
            input_len=96,
            pred_len=96,
            input_vars=1
    ):
        super(NBeats, self).__init__()
        self.enc_in = input_vars
        self.block = block_dict[block_type]
        self.model = NBeatsUnivariable(
            torch.nn.ModuleList(
                [NBeatsBlock(input_size=input_len,
                             theta_size=input_len + pred_len,
                             basis_function=self.block(backcast_size=input_len, forecast_size=pred_len),
                             layers=inner_layers,
                             layer_size=d_model)
                 for _ in range(outer_layers)]
            )
        )

    def forward(self, x, *args):
        # x: [Batch, Input length, Channel]
        res = []
        for i in range(self.enc_in):
            dec_out = self.model(x[:, :, i])
            res.append(dec_out)
        return torch.stack(res, dim=-1)  # to [Batch, Output length, Channel]
