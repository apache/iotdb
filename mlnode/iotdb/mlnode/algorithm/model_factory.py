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


from iotdb.mlnode.algorithm.models.DLinear import *
from iotdb.mlnode.algorithm.models.NBeats import *


support_forecasting_model = []
support_forecasting_model.extend(DLinear.support_model_names)
support_forecasting_model.extend(NBeats.support_model_names)

"""
Common configs for all forecasting model with default values
"""


def _common_cfg(**kwargs):
    return {
        'input_len': 96,
        'pred_len': 96,
        'input_vars': 1,
        'output_vars': 1,
        **kwargs
    }


"""
Common forecasting task configs
"""
support_common_cfgs = {
    # univariate forecasting
    's': _common_cfg(
        input_vars=1,
        output_vars=1),

    # univariate forecasting with observable exogenous variables
    'ms': _common_cfg(
        output_vars=1),

    # multivariate forecasting, current support this only
    'm': _common_cfg(),
}


def is_model(model_name: str) -> bool:
    """ 
    Check if a model name exists
    """
    return model_name in support_forecasting_model


def list_model():
    """ 
    List support forecasting model
    """
    return support_forecasting_model


def create_forecast_model(
        model_name,
        task_type='m',
        input_len=96,
        pred_len=96,
        input_vars=1,
        output_vars=1,
        **kwargs,
):
    """ 
    Factory method for all support forecasting models
    the given arguments is common configs shared by all forecasting models 
    for specific model configs, see __model_cfg in `algorithm/models/MODELNAME.py`

    Args:
        model_name: see available models by `list_model`
        task_type: 'm' for multivariate forecasting, 'ms' for covariate forecasting,
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
        raise RuntimeError(f'Unknown forecasting model: ({model_name}),'
                           f' which should be one of {list_model()}')
    if task_type not in support_common_cfgs.keys():
        raise RuntimeError(f'Unknown forecasting task type: ({task_type})'
                           f' which should be one of {support_common_cfgs.keys()}')

    common_cfg = support_common_cfgs[task_type]
    common_cfg['input_len'] = input_len
    common_cfg['pred_len'] = pred_len
    common_cfg['input_vars'] = input_vars
    common_cfg['output_vars'] = output_vars
    assert input_len > 0 and pred_len > 0, 'Length of input/output series should be positive'
    assert input_vars > 0 and output_vars > 0, 'Number of input/output series should be positive'

    create_fn = eval(model_name)
    model, model_config = create_fn(
        common_config=common_cfg,
        **kwargs
    )
    model_config['task_type'] = task_type
    model_config['model_name'] = model_name

    return model, model_config


