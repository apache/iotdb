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

from iotdb.mlnode.algorithm.enums import ForecastModelType, ForecastTaskType
from iotdb.mlnode.algorithm.models.forecast.dlinear import (dlinear,
                                                            dlinear_individual)
from iotdb.mlnode.algorithm.models.forecast.nbeats import nbeats
from iotdb.mlnode.exception import BadConfigValueError
# Common configs for all forecasting model with default values
from iotdb.mlnode.parser import TaskOptions


def _common_config(**kwargs):
    return {
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 1,
        'output_vars': 1,
        **kwargs
    }


# Common forecasting task configs
_forecasting_model_default_config_dict = {
    # multivariable forecasting with all endogenous variables, current support this only
    ForecastTaskType.ENDOGENOUS: _common_config(
        input_vars=1,
        output_vars=1),

    # multivariable forecasting with some exogenous variables
    ForecastTaskType.EXOGENOUS: _common_config(
        output_vars=1),
}


def create_forecast_model(
        task_options: TaskOptions,
        model_configs: Dict,
) -> nn.Module:
    """
    Factory method for all support forecasting models
    the given arguments is common configs shared by all forecasting models
    for specific model configs, see _model_config in `MODELNAME.py`

    Args:
        model_name: see available models by `list_model`
        forecast_task_type: see algorithm/enums for available choices
        input_len: time length of model input
        pred_len: time length of model output
        input_vars: number of input series
        output_vars: number of output series
        kwargs: for specific model configs, see returned `model_config` with kwargs=None

    Returns:
        model: torch.nn.Module
        model_config: dict of model configurations
    """
    if model_name not in ForecastModelType.values():
        raise BadConfigValueError('model_name', model_name, f'It should be one of {ForecastModelType.values()}')
    if forecast_task_type not in _forecasting_model_default_config_dict.keys():
        raise BadConfigValueError('forecast_task_type', forecast_task_type,
                                  f'It should be one of {list(_forecasting_model_default_config_dict.keys())}')

    if not input_len > 0:
        raise BadConfigValueError('input_len', input_len,
                                  'Length of input series should be positive')
    if not pred_len > 0:
        raise BadConfigValueError('pred_len', pred_len,
                                  'Length of predicted series should be positive')
    if not input_vars > 0:
        raise BadConfigValueError('input_vars', input_vars,
                                  'Number of input variables should be positive')
    if not output_vars > 0:
        raise BadConfigValueError('output_vars', output_vars,
                                  'Number of output variables should be positive')
    if forecast_task_type is ForecastTaskType.ENDOGENOUS:
        if input_vars != output_vars:
            raise BadConfigValueError('forecast_task_type', forecast_task_type,
                                      'Number of input/output variables should be '
                                      'the same in endogenous forecast')

    model_type =
    if task_options.model_type == ForecastModelType.DLINEAR.value:
        model, model_config = dlinear(
            common_config=common_config,
            **kwargs
        )
    elif task_options.model_type == ForecastModelType.DLINEAR_INDIVIDUAL.value:
        model, model_config = dlinear_individual(
            common_config=common_config,
            **kwargs
        )
    elif task_options.model_type == ForecastModelType.NBEATS.value:
        model, model_config = nbeats(
            common_config=common_config,
            **kwargs
        )
    else:
        raise BadConfigValueError('model_name', task_options.model_type,
                                  f'It should be one of {ForecastModelType.values()}')

    return model
