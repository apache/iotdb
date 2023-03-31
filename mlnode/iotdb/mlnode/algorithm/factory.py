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
import torch.nn as nn

from iotdb.mlnode.algorithm.enums import ForecastTaskType
from iotdb.mlnode.algorithm.models.forecast import support_forecasting_models
from iotdb.mlnode.exception import BadConfigValueError


# Common configs for all forecasting model with default values
def _common_config(**kwargs):
    return {
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 1,
        'output_vars': 1,
        **kwargs
    }


# Common forecasting task configs
support_common_configs = {
    # multivariate forecasting, current support this only
    ForecastTaskType.ENDOGENOUS: _common_config(
        input_vars=1,
        output_vars=1),

    # univariate forecasting with observable exogenous variables
    ForecastTaskType.EXOGENOUS: _common_config(
        output_vars=1),
}


def is_model(model_name: str) -> bool:
    """
    Check if a model name exists
    """
    return model_name in support_forecasting_models


def list_model() -> list[str]:
    """
    List support forecasting model
    """
    return support_forecasting_models


def create_forecast_model(
        model_name,
        forecast_task_type=ForecastTaskType.ENDOGENOUS,
        input_len=96,
        pred_len=96,
        input_vars=1,
        output_vars=1,
        **kwargs,
) -> [nn.Module, dict]:
    """
    Factory method for all support forecasting models
    the given arguments is common configs shared by all forecasting models
    for specific model configs, see __model_config in `algorithm/models/MODELNAME.py`

    Args:
        model_name: see available models by `list_model`
        forecast_task_type: 'm' for multivariate forecasting, 'ms' for covariate forecasting,
                   's' for univariate forecasting
        input_len: time length of model input
        pred_len: time length of model output
        input_vars: number of input series
        output_vars: number of output series
        kwargs: for specific model configs, see returned `model_config` with kwargs=None

    Returns:
        model: torch.nn.Module
        model_config: dict of model configurations
    """
    if not is_model(model_name):
        raise BadConfigValueError('model_name', model_name, f'It should be one of {list_model()}')
    if forecast_task_type not in support_common_configs.keys():
        raise BadConfigValueError('forecast_task_type', forecast_task_type,
                                  f'It should be one of {list(support_common_configs.keys())}')

    common_config = support_common_configs[forecast_task_type]
    common_config['input_len'] = input_len
    common_config['pred_len'] = pred_len
    common_config['input_vars'] = input_vars
    common_config['output_vars'] = output_vars
    common_config['forecast_task_type'] = str(forecast_task_type)

    if not input_len > 0:
        raise BadConfigValueError('input_len', input_len,
                                  'Length of input series should be positive')
    if not pred_len > 0:
        raise BadConfigValueError('pred_len', pred_len,
                                  'Length of predicted series should be positive')
    if not input_vars > 0:
        raise BadConfigValueError('input_vars', input_vars,
                                  'Number of input variates should be positive')
    if not output_vars > 0:
        raise BadConfigValueError('output_vars', output_vars,
                                  'Number of output variates should be positive')
    if forecast_task_type == ForecastTaskType.ENDOGENOUS:
        if input_vars != output_vars:
            raise BadConfigValueError('forecast_task_type', forecast_task_type,
                                      'Number of input/output variates should be '
                                      'the same in multivariate forecast')
    create_fn = eval(model_name)
    model, model_config = create_fn(
        common_config=common_config,
        **kwargs
    )
    model_config['model_name'] = model_name

    return model, model_config
