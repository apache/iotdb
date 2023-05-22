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


import argparse
import re
from typing import Dict, List, Tuple

from iotdb.mlnode.algorithm.enums import ForecastTaskType
from iotdb.mlnode.data_access.enums import DatasetType, DataSourceType
from iotdb.mlnode.exception import MissingConfigError, WrongTypeConfigError
from iotdb.mlnode.serde import convert_to_df
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq, TForecastReq


class _ConfigParser(argparse.ArgumentParser):
    """
    A parser for parsing configs from configs: dict
    """

    def __init__(self):
        super().__init__()

    def parse_configs(self, configs) -> Dict:
        """
        Parse configs from a dict
        Args:configs: a dict of all configs which contains all required arguments
        Returns: a dict of parsed configs
        """
        args = self.parse_dict(configs)
        return vars(self.parse_known_args(args)[0])

    @staticmethod
    def parse_dict(config_dict: Dict) -> List:
        """
        Parse a dict of configs to a list of arguments
        Args:config_dict: a dict of configs
        Returns: a list of arguments which can be parsed by argparse
        """
        args = []
        for k, v in config_dict.items():
            if v is None:
                continue
            args.append("--{}".format(k))
            if isinstance(v, str) and re.match(r'^\[(.*)]$', v):
                v = eval(v)
                v = [str(i) for i in v]
                args.extend(v)
            elif isinstance(v, list):
                args.extend([str(i) for i in v])
            elif isinstance(v, bool):
                args.append(str(v).lower())
            else:
                args.append(v)
        return args

    def error(self, message: str):
        """
        Override the error method to raise exceptions instead of exiting
        """
        if message.startswith('the following arguments are required:'):
            missing_arg = re.findall(r': --(\w+)', message)[0]
            raise MissingConfigError(missing_arg)
        elif re.match(r'argument --\w+: invalid \w+ value:', message):
            argument = re.findall(r'argument --(\w+):', message)[0]
            expected_type = re.findall(r'invalid (\w+) value:', message)[0]
            raise WrongTypeConfigError(argument, expected_type)
        else:
            raise Exception(message)


def str2bool(value):
    if value.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif value.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


""" Argument description:
 - query_expressions: query expressions
 - query_filter: query filter
 - source_type: source type
 - filename: filename
 - dataset_type: dataset type
 - time_embed: freq for time features encoding
 - input_len: input sequence length
 - pred_len: prediction sequence length
 - input_vars: number of input variables
 - output_vars: number of output variables
"""
_data_config_parser = _ConfigParser()
_data_config_parser.add_argument('--source_type',
                                 type=DataSourceType,
                                 default=DataSourceType.THRIFT,
                                 choices=list(DataSourceType))
_data_config_parser.add_argument('--dataset_type',
                                 type=DatasetType,
                                 default=DatasetType.WINDOW,
                                 choices=list(DatasetType))
_data_config_parser.add_argument('--filename', type=str, default='')
_data_config_parser.add_argument('--query_expressions', type=str, nargs='*', default=[])
_data_config_parser.add_argument('--query_filter', type=str, default=None)
_data_config_parser.add_argument('--time_embed', type=str, default='h')
_data_config_parser.add_argument('--input_len', type=int, default=96)
_data_config_parser.add_argument('--pred_len', type=int, default=96)

""" Argument description:
 - model_name: model name
 - input_len: input sequence length
 - pred_len: prediction sequence length
 - input_vars: number of input variables
 - output_vars: number of output variables
 - task_type: task type, options:[M, S, MS];
        M:multivariate predict multivariate,
        S:univariate predict univariate,
        MS:multivariate predict univariate'
 - kernel_size: kernel size
 - block_type: block type
 - d_model: dimension of feature in model
 - inner_layers: number of inner layers
 - outer_layers: number of outer layers
"""
_model_config_parser = _ConfigParser()
_model_config_parser.add_argument('--model_name', type=str, required=True)
_model_config_parser.add_argument('--input_len', type=int, default=96)
_model_config_parser.add_argument('--pred_len', type=int, default=96)
_model_config_parser.add_argument('--forecast_task_type',
                                  type=ForecastTaskType,
                                  default=ForecastTaskType.ENDOGENOUS,
                                  choices=list(ForecastTaskType))
_model_config_parser.add_argument('--kernel_size', type=int, default=25)
_model_config_parser.add_argument('--block_type', type=str, default='generic')
_model_config_parser.add_argument('--d_model', type=int, default=128)
_model_config_parser.add_argument('--inner_layers', type=int, default=4)
_model_config_parser.add_argument('--outer_layers', type=int, default=4)

""" Argument description:
 - model_id: model id
 - tuning: whether to tune hyperparameters
 - task_type: task type, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate,
        MS:multivariate predict univariate'
 - task_class: task class
 - input_len: input sequence length
 - pred_len: prediction sequence length
 - input_vars: number of input variables
 - output_vars: number of output variables
 - learning_rate: learning rate
 - batch_size: batch size
 - num_workers: number of workers
 - epochs: number of epochs
 - use_gpu: whether to use gpu
 - use_multi_gpu: whether to use multi-gpu
 - devices: devices to use
 - metric_names: metric to use
"""
_task_config_parser = _ConfigParser()
_task_config_parser.add_argument('--task_class', type=str, required=True)
_task_config_parser.add_argument('--model_id', type=str, required=True)
_task_config_parser.add_argument('--tuning', type=str2bool, default=False)
_task_config_parser.add_argument('--forecast_task_type',
                                 type=ForecastTaskType,
                                 default=ForecastTaskType.ENDOGENOUS,
                                 choices=list(ForecastTaskType))
_task_config_parser.add_argument('--input_len', type=int, default=96)
_task_config_parser.add_argument('--pred_len', type=int, default=96)
_task_config_parser.add_argument('--learning_rate', type=float, default=0.0001)
_task_config_parser.add_argument('--batch_size', type=int, default=32)
_task_config_parser.add_argument('--num_workers', type=int, default=0)
_task_config_parser.add_argument('--epochs', type=int, default=10)
_task_config_parser.add_argument('--use_gpu', type=str2bool, default=False)
# _task_config_parser.add_argument('--gpu', type=int, default=0)
# _task_config_parser.add_argument('--use_multi_gpu', type=str2bool, default=False)
# _task_config_parser.add_argument('--devices', type=int, nargs='+', default=[0])
_task_config_parser.add_argument('--metric_names', type=str, nargs='+', default=['MSE'])


def parse_training_request(req: TCreateTrainingTaskReq) -> Tuple[Dict, Dict, Dict]:
    """
    Parse TCreateTrainingTaskReq with given yaml template
    Args:
        req: TCreateTrainingTaskReq
    Returns:
        data_config: configurations related to data
        model_config: configurations related to model
        task_config: configurations related to task
    """
    config = req.modelConfigs
    config.update(model_name=config['model_type'])
    config.update(task_class=config['model_task'])
    config.update(model_id=req.modelId)
    config.update(tuning=req.isAuto)
    config.update(query_expressions=req.queryExpressions)
    config.update(query_filter=req.queryFilter)

    data_config = _data_config_parser.parse_configs(config)
    model_config = _model_config_parser.parse_configs(config)
    task_config = _task_config_parser.parse_configs(config)
    return data_config, model_config, task_config


def parse_forecast_request(req: TForecastReq):
    model_path = req.modelPath
    column_name_list = req.inputColumnNameList
    column_type_list = req.inputTypeList
    ts_dataset = req.inputData
    pred_len = req.predictLength

    data = convert_to_df(column_name_list, column_type_list, None, [ts_dataset])
    time_stamp, data = data[data.columns[0:1]], data[data.columns[1:]]
    full_data = (data, time_stamp)
    return model_path, full_data, pred_len
