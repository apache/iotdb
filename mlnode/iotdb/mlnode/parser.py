import argparse


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
    def __init__(self, config_type='data', *args, **kwargs):
        super(ConfigParser, self).__init__(*args, **kwargs)
        if config_type == 'data':
            self.add_argument('--source_type', type=str, default='file')
            self.add_argument('--filename', type=str, default='dataset/ETT-small/ETTh1.csv')
            self.add_argument('--query_expressions', type=list, default=['root.eg.etth1.s0', 'root.eg.etth1.s1'])
            self.add_argument('--query_filter', type=str, default='-1,1501516800000')
            self.add_argument('--dataset_type', type=str, default='window')
            self.add_argument('--time_embed', type=str, default='h')
            self.add_argument('--input_len', type=int, default=96)
            self.add_argument('--pred_len', type=int, default=96)
            self.add_argument('--input_vars', type=int, default=7)
            self.add_argument('--output_vars', type=int, default=7)
        elif config_type == 'model':
            self.add_argument('--model_name', type=str, default='dlinear')
            self.add_argument('--input_len', type=int, default=96)
            self.add_argument('--pred_len', type=int, default=96)
            self.add_argument('--input_vars', type=int, default=7)
            self.add_argument('--output_vars', type=int, default=7)
            self.add_argument('--task_type', type=str, default='m')
            self.add_argument('--kernel_size', type=int, default=25)
            self.add_argument('--block_type', type=str, default='g')
            self.add_argument('--d_model', type=int, default=128)
            self.add_argument('--inner_layers', type=int, default=4)
            self.add_argument('--outer_layers', type=int, default=4)
        elif config_type == 'task':
            self.add_argument('--model_id', type=str, default='mid_test')
            self.add_argument('--tuning', type=bool, default=False)
            self.add_argument('--task_type', type=str, default='m')
            self.add_argument('--task_class', type=str, default='forecast_training_task')
            self.add_argument('--input_len', type=int, default=96)
            self.add_argument('--pred_len', type=int, default=96)
            self.add_argument('--input_vars', type=int, default=7)
            self.add_argument('--output_vars', type=int, default=7)
            self.add_argument('--learning_rate', type=float, default=0.0001)
            self.add_argument('--batch_size', type=int, default=32)
            self.add_argument('--num_workers', type=int, default=0)
            self.add_argument('--epochs', type=int, default=10)
            self.add_argument('--use_gpu', type=bool, default=False)
            self.add_argument('--gpu', type=int, default=0)
            self.add_argument('--use_multi_gpu', type=bool, default=False)
            self.add_argument('--devices', type=list, default=[0])
            self.add_argument('--metric_names', type=list, default=['MSE', 'MAE'])
        else:
            raise NotImplementedError
