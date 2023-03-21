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
from typing import Tuple
from iotdb.mlnode.exception import BadConfigError

__all__ = ['NBeats', 'nbeats']  # , 'nbeats_s', 'nbeats_t']

"""
Specific configs for NBeats with default values
"""


def _model_cfg(**kwargs):
    return {
        'block_type': 'g',
        'd_model': 128,
        'inner_layers': 4,
        'outer_layers': 4,
        # 'harmonics': 4,
        # 'degree_of_polynomial': 3,
        **kwargs
    }


"""
Specific configs for NBeats variants
"""
support_model_cfgs = {
    'nbeats': _model_cfg(
        block_type='g'),
    # 'nbeats_s': _model_cfg(
    #     harmonics=4,
    #     block_type='s'),
    # 'nbeats_t': _model_cfg(
    #     degree_of_polynomial=3,
    #     block_type='t')
}


class GenericBasis(nn.Module):
    """
    Generic basis function.
    """

    def __init__(self, backcast_size: int, forecast_size: int):
        super().__init__()
        self.backcast_size = backcast_size
        self.forecast_size = forecast_size

    def forward(self, theta: torch.Tensor):
        return theta[:, :self.backcast_size], theta[:, -self.forecast_size:]


# class TrendBasis(nn.Module):
#     """
#     Trend basis function.
#     """
#     def __init__(self, degree_of_polynomial: int, backcast_size: int, forecast_size):
#         super().__init__()
#         polynomial_size = degree_of_polynomial + 1
#         self.backcast_basis = nn.Parameter(
#             torch.tensor(np.concatenate([np.power(np.arange(backcast_size, dtype=float) / backcast_size, i)[None, :]
#                                     for i in range(polynomial_size)]), dtype=torch.float32), requires_grad=False)
#         self.forecast_basis = nn.Parameter(
#             torch.tensor(np.concatenate([np.power(np.arange(forecast_size, dtype=float) / forecast_size, i)[None, :]
#                                     for i in range(polynomial_size)]), dtype=torch.float32), requires_grad=False)

#     def forward(self, theta):
#         cut_point = self.forecast_basis.shape[0]
#         backcast = torch.einsum('bp,pt->bt', theta[:, cut_point:], self.backcast_basis)
#         forecast = torch.einsum('bp,pt->bt', theta[:, :cut_point], self.forecast_basis)
#         return backcast, forecast


# class SeasonalityBasis(nn.Module):
#     """
#     Seasonality basis function.
#     """
#     def __init__(self, harmonics: int, backcast_size: int, forecast_size: int):
#         super().__init__()
#         frequency = np.append(np.zeros(1, dtype=float),
#                                         np.arange(harmonics, harmonics / 2 * forecast_size,
#                                                     dtype=float) / harmonics)[None, :]
#         backcast_grid = -2 * np.pi * (
#                 np.arange(backcast_size, dtype=float)[:, None] / forecast_size) * frequency
#         forecast_grid = 2 * np.pi * (
#                 np.arange(forecast_size, dtype=float)[:, None] / forecast_size) * frequency

#         backcast_cos_template = torch.tensor(np.transpose(np.cos(backcast_grid)), dtype=torch.float32)
#         backcast_sin_template = torch.tensor(np.transpose(np.sin(backcast_grid)), dtype=torch.float32)
#         backcast_template = torch.cat([backcast_cos_template, backcast_sin_template], dim=0)

#         forecast_cos_template = torch.tensor(np.transpose(np.cos(forecast_grid)), dtype=torch.float32)
#         forecast_sin_template = torch.tensor(np.transpose(np.sin(forecast_grid)), dtype=torch.float32)
#         forecast_template = torch.cat([forecast_cos_template, forecast_sin_template], dim=0)

#         self.backcast_basis = nn.Parameter(backcast_template, requires_grad=False)
#         self.forecast_basis = nn.Parameter(forecast_template, requires_grad=False)

#     def forward(self, theta):
#         cut_point = self.forecast_basis.shape[0]
#         backcast = torch.einsum('bp,pt->bt', theta[:, cut_point:], self.backcast_basis)
#         forecast = torch.einsum('bp,pt->bt', theta[:, :cut_point], self.forecast_basis)
#         return backcast, forecast


class NBeatsBlock(nn.Module):
    """
    N-BEATS block which takes a basis function as an argument
    """

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
        self.layers = nn.ModuleList([nn.Linear(in_features=input_size, out_features=layer_size)] +
                                    [nn.Linear(in_features=layer_size, out_features=layer_size)
                                     for _ in range(layers - 1)])
        self.basis_parameters = nn.Linear(in_features=layer_size, out_features=theta_size)
        self.basis_function = basis_function

    def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        block_input = x
        for layer in self.layers:
            block_input = torch.relu(layer(block_input))
        basis_parameters = self.basis_parameters(block_input)
        return self.basis_function(basis_parameters)


class NBeatsUnivar(nn.Module):
    """
    N-Beats Model.
    """

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


block_dict = {
    'g': GenericBasis,
    # 't': TrendBasis,
    # 's': SeasonalityBasis,
}


class NBeats(nn.Module):
    """
    Neural Basis Expansion Analysis Time Series
    """
    support_model_names = support_model_cfgs.keys()

    def __init__(
            self,
            block_type='g',
            d_model=128,
            inner_layers=4,
            outer_layers=4,
            # harmonics=4,
            # degree_of_polynomial=3,
            input_len=96,
            pred_len=96,
            input_vars=1,
            output_vars=1,
            task_type='m',  # TODO, support ms
            model_name='nbeats',
    ):
        super(NBeats, self).__init__()

        self.enc_in = input_vars
        self.block = block_dict[block_type]
        self.task_type = task_type
        self.model_name = model_name

        self.model = NBeatsUnivar(torch.nn.ModuleList([NBeatsBlock(input_size=input_len,
                                                                   theta_size=input_len + pred_len,
                                                                   basis_function=self.block(backcast_size=input_len,
                                                                                             forecast_size=pred_len,
                                                                                             # harmonics=harmonics,
                                                                                             # degree_of_polynomial=degree_of_polynomial
                                                                                             ),
                                                                   layers=inner_layers,
                                                                   layer_size=d_model)
                                                       for _ in range(outer_layers)]))

    def forward(self, x, *args):
        # x: [Batch, Input length, Channel]
        res = []
        for i in range(self.enc_in):
            dec_out = self.model(x[:, :, i])
            res.append(dec_out)
        return torch.stack(res, dim=-1)  # to [Batch, Output length, Channel]


def nbeats(common_config, d_model=128, inner_layers=4, outer_layers=4, **kwargs):
    cfg = support_model_cfgs['nbeats']
    cfg.update(**common_config)
    if d_model <= 0:
        raise BadConfigError('Model dimension (d_model) of nbeats should be positive')
    if inner_layers <= 0 or outer_layers <= 0:
        raise BadConfigError('Number of inner/outer layers of nbeats should be positive')
    cfg['d_model'] = d_model
    cfg['inner_layers'] = inner_layers
    cfg['outer_layers'] = outer_layers
    return NBeats(**cfg), cfg

# #TODO: test model usability
def nbeats_s(d_model=128, inner_layers=4, outer_layers=4, harmonics=4, **kwargs):
    raise NotImplementedError

# #TODO: test model usability
def nbeats_t(d_model=128, inner_layers=4, outer_layers=4, degree_of_polynomial=3, **kwargs):
    raise NotImplementedError
