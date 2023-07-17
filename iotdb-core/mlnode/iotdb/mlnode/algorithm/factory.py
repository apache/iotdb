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

from typing import Dict

import torch.nn as nn

from iotdb.mlnode.algorithm.hyperparameter import HyperparameterName
from iotdb.mlnode.algorithm.models.forecast.dlinear import (DLinear,
                                                            DLinearIndividual)
from iotdb.mlnode.algorithm.models.forecast.nbeats import NBeats
from iotdb.mlnode.constant import ForecastModelType
from iotdb.mlnode.exception import BadConfigValueError
from iotdb.mlnode.parser import ForecastTaskOptions


def create_forecast_model(
        task_options: ForecastTaskOptions,
        model_configs: Dict,
) -> nn.Module:
    """
    Factory method for all support forecasting models
    the given arguments is common configs shared by all forecasting models
    """
    if task_options.model_type.value not in ForecastModelType.values():
        raise BadConfigValueError('model_name', f'It should be one of {ForecastModelType.values()}')

    if task_options.model_type == ForecastModelType.DLINEAR:
        return DLinear(kernel_size=model_configs[HyperparameterName.KERNEL_SIZE.value],
                       input_len=task_options.input_length,
                       pred_len=task_options.predict_length,
                       input_vars=model_configs[HyperparameterName.INPUT_VARS.value])
    elif task_options.model_type == ForecastModelType.DLINEAR_INDIVIDUAL:
        return DLinearIndividual(kernel_size=model_configs[HyperparameterName.KERNEL_SIZE.value],
                                 input_len=task_options.input_length,
                                 pred_len=task_options.predict_length,
                                 input_vars=model_configs[HyperparameterName.INPUT_VARS.value]
                                 )
    elif task_options.model_type == ForecastModelType.NBEATS:
        return NBeats(
            block_type=model_configs[HyperparameterName.BLOCK_TYPE.value],
            d_model=model_configs[HyperparameterName.D_MODEL.value],
            inner_layers=model_configs[HyperparameterName.INNER_LAYERS.value],
            outer_layers=model_configs[HyperparameterName.OUTER_LAYERS.value],
            input_len=task_options.input_length,
            pred_len=task_options.predict_length,
            input_vars=model_configs[HyperparameterName.INPUT_VARS.value]
        )
    else:
        raise BadConfigValueError('model_name', task_options.model_type,
                                  f'It should be one of {ForecastModelType.values()}')
