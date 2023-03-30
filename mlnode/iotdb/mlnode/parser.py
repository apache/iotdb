import argparse
import re
import sys

from iotdb.mlnode.exception import MissingConfigError, WrongTypeError


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


class ConfigParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__()

    def parse_configs(self, configs):
        args = self.parse_dict(configs)
        return vars(self.parse_known_args(args)[0])

    @staticmethod
    def parse_dict(config_dict):
        args = []
        for k, v in config_dict.items():
            args.append("--{}".format(k))
            if isinstance(v, str) and re.match(r'^\[(.*)\]$', v):
                v = eval(v)
                v = [str(i) for i in v]
                args.extend(v)
            elif isinstance(v, list):
                args.extend([str(i) for i in v])
            else:
                args.append(v)
        return args

    def error(self, message: str):
        # required arguments are missing
        if message.startswith('the following arguments are required:'):
            missing_arg = re.findall(r': --(\w+)', message)[0]
            raise MissingConfigError(missing_arg)
        elif re.match(r'argument --\w+: invalid \w+ value:', message):
            argument = re.findall(r'argument --(\w+):', message)[0]
            expected_type = re.findall(r'invalid (\w+) value:', message)[0]
            raise WrongTypeError(argument, expected_type)

        sys.exit()


def get_configs(config):
    data_config_parser = ConfigParser()
    model_config_parser = ConfigParser()
    task_config_parser = ConfigParser()
    data_config_parser.add_argument('--query_expressions', type=str, nargs='*', default=[])
    data_config_parser.add_argument('--query_filter', type=str, default='')
    data_config_parser.add_argument('--source_type', type=str, default='')
    data_config_parser.add_argument('--filename', type=str, default='')
    data_config_parser.add_argument('--dataset_type', type=str, default='window')
    data_config_parser.add_argument('--time_embed', type=str, default='h')
    data_config_parser.add_argument('--input_len', type=int, default=96)
    data_config_parser.add_argument('--pred_len', type=int, default=96)
    data_config_parser.add_argument('--input_vars', type=int, required=True)
    data_config_parser.add_argument('--output_vars', type=int, required=True)

    model_config_parser.add_argument('--model_name', type=str, required=True)
    model_config_parser.add_argument('--input_len', type=int, default=96)
    model_config_parser.add_argument('--pred_len', type=int, default=96)
    model_config_parser.add_argument('--input_vars', type=int, required=True)
    model_config_parser.add_argument('--output_vars', type=int, required=True)
    model_config_parser.add_argument('--task_type', type=str, default='m')
    model_config_parser.add_argument('--kernel_size', type=int, default=25)
    model_config_parser.add_argument('--block_type', type=str, default='g')
    model_config_parser.add_argument('--d_model', type=int, default=128)
    model_config_parser.add_argument('--inner_layers', type=int, default=4)
    model_config_parser.add_argument('--outer_layers', type=int, default=4)

    task_config_parser.add_argument('--model_id', type=str, default='')
    task_config_parser.add_argument('--tuning', type=bool, default=False)
    task_config_parser.add_argument('--task_type', type=str, default='m')
    task_config_parser.add_argument('--task_class', type=str, default='forecast_training_task', required=True)
    task_config_parser.add_argument('--input_len', type=int, default=96)
    task_config_parser.add_argument('--pred_len', type=int, default=96)
    task_config_parser.add_argument('--input_vars', type=int, default=7, required=True)
    task_config_parser.add_argument('--output_vars', type=int, default=7, required=True)
    task_config_parser.add_argument('--learning_rate', type=float, default=0.0001)
    task_config_parser.add_argument('--batch_size', type=int, default=32)
    task_config_parser.add_argument('--num_workers', type=int, default=0)
    task_config_parser.add_argument('--epochs', type=int, default=10)
    task_config_parser.add_argument('--use_gpu', type=bool, default=False)
    task_config_parser.add_argument('--gpu', type=int, default=0)
    task_config_parser.add_argument('--use_multi_gpu', type=bool, default=False)
    task_config_parser.add_argument('--devices', type=list, default=[0])
    task_config_parser.add_argument('--metric_names', type=str, nargs='+', default=['MSE', 'MAE'])

    return data_config_parser.parse_configs(config), \
        model_config_parser.parse_configs(config), \
        task_config_parser.parse_configs(config)
