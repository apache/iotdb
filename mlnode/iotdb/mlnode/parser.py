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

from iotdb.mlnode.exception import (MissingConfigError, ModelNotSupportedError,
                                    WrongTypeError)


class ConfigParser(object):
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.argument_list = []
        self.required_list = []

    def parse_configs(self, config):
        conf_dict = vars(self.parser.parse_args([]))
        args = []
        for k in self.required_list:
            if k not in config.keys():
                raise MissingConfigError(k)
        for k, v in config.items():
            if k in conf_dict.keys():
                args.append("--{}".format(k))
                args.append(eval(v) if type(conf_dict[k]) is list and type(v) is not list else v)
                if type(conf_dict[k]) is int or type(conf_dict[k]) is float:
                    if (type(eval(args[-1])) is int) and (type(conf_dict[k]) is float) or \
                            (type(eval(args[-1])) is float) and (type(conf_dict[k]) is int):
                        raise WrongTypeError(k, type(conf_dict[k]), type(eval(args[-1])))

        return vars(self.parser.parse_args(args))


class DataConfigParser(ConfigParser):
    def __init__(self):
        super().__init__()
        self.parser.add_argument('--query_expressions', type=list, default=[])
        self.parser.add_argument('--query_filter', type=str, default='')
        self.parser.add_argument('--source_type', type=str, default='')
        self.parser.add_argument('--filename', type=str, default='')
        self.parser.add_argument('--dataset_type', type=str, default='window')
        self.parser.add_argument('--time_embed', type=str, default='h')
        self.parser.add_argument('--input_len', type=int, default=96)
        self.parser.add_argument('--pred_len', type=int, default=96)
        self.parser.add_argument('--input_vars', type=int, default=7)
        self.parser.add_argument('--output_vars', type=int, default=7)
        self.argument_list = ['source_type', 'filename', 'dataset_type', 'time_embed',
                              'input_len', 'pred_len', 'input_vars', 'output_vars', 'query_expressions', 'query_filter']
        self.required_list = ['source_type', 'filename', 'input_vars',
                              'output_vars', 'query_expressions', 'query_filter']


class TaskConfigParser(ConfigParser):
    def __init__(self):
        super().__init__()
        self.parser.add_argument('--model_id', type=str, default='')
        self.parser.add_argument('--tuning', type=bool, default=False)
        self.parser.add_argument('--task_type', type=str, default='m')
        self.parser.add_argument('--task_class', type=str, default='forecast_training_task')
        self.parser.add_argument('--input_len', type=int, default=96)
        self.parser.add_argument('--pred_len', type=int, default=96)
        self.parser.add_argument('--input_vars', type=int, default=7)
        self.parser.add_argument('--output_vars', type=int, default=7)
        self.parser.add_argument('--learning_rate', type=float, default=0.0001)
        self.parser.add_argument('--batch_size', type=int, default=32)
        self.parser.add_argument('--num_workers', type=int, default=0)
        self.parser.add_argument('--epochs', type=int, default=10)
        self.parser.add_argument('--use_gpu', type=bool, default=False)
        self.parser.add_argument('--gpu', type=int, default=0)
        self.parser.add_argument('--use_multi_gpu', type=bool, default=False)
        self.parser.add_argument('--devices', type=list, default=[0])
        self.parser.add_argument('--metric_names', type=list, default=['MSE', 'MAE'])
        self.argument_list = ['model_id', 'tuning', 'task_type', 'task_class', 'input_len', 'pred_len',
                              'input_vars', 'output_vars', 'learning_rate', 'batch_size', 'num_workers',
                              'epochs', 'use_gpu', 'gpu', 'use_multi_gpu', 'devices', 'metric_names']
        self.required_list = ['model_id', 'input_vars', 'output_vars']


class DLinearConfigParser(ConfigParser):
    def __init__(self):
        super().__init__()
        self.parser.add_argument('--model_name', type=str, default='dlinear')
        self.parser.add_argument('--input_len', type=int, default=96)
        self.parser.add_argument('--pred_len', type=int, default=96)
        self.parser.add_argument('--input_vars', type=int, default=7)
        self.parser.add_argument('--output_vars', type=int, default=7)
        self.parser.add_argument('--task_type', type=str, default='m')
        self.parser.add_argument('--kernel_size', type=int, default=25)
        self.parser.add_argument('--block_type', type=str, default='g')
        self.parser.add_argument('--d_model', type=int, default=128)
        self.parser.add_argument('--inner_layers', type=int, default=4)
        self.parser.add_argument('--outer_layers', type=int, default=4)
        self.argument_list = ['model_name', 'input_len', 'pred_len', 'input_vars', 'output_vars',
                              'task_type', 'kernel_size', 'block_type', 'd_model', 'inner_layers', 'outer_layers']
        self.required_list = ['model_name', 'input_vars', 'output_vars']


class ConfigManager(object):
    def __init__(self, config):
        self.config = config
        if 'model_name' not in config:
            raise MissingConfigError('model_name')
        self.data_conf_parser = DataConfigParser()
        self.task_conf_parser = TaskConfigParser()
        self.model_conf_parser = get_model_config_parser(config['model_name'])

    def get_configs(self):
        data_conf = self.data_conf_parser.parse_configs(self.config)
        task_conf = self.task_conf_parser.parse_configs(self.config)
        model_conf = self.model_conf_parser.parse_configs(self.config)
        return data_conf, task_conf, model_conf


def get_model_config_parser(model_name):
    parser = None
    if model_name == 'dlinear':
        parser = DLinearConfigParser()
    else:
        raise ModelNotSupportedError(model_name)
    return parser
