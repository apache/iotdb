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


from iotdb.mlnode.exception import BadNodeUrlError
from iotdb.thrift.common.ttypes import TEndPoint
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import ConfigParser


def parse_endpoint_url(endpoint_url: str) -> TEndPoint:
    """ Parse TEndPoint from a given endpoint url.
    Args:
        endpoint_url: an endpoint url, format: ip:port
    Returns:
        TEndPoint
    Raises:
        BadNodeUrlError
    """
    split = endpoint_url.split(":")
    if len(split) != 2:
        logger.warning("Illegal endpoint url format: {}".format(endpoint_url))
        raise BadNodeUrlError(endpoint_url)

    ip = split[0]
    try:
        port = int(split[1])
        result = TEndPoint(ip, port)
        return result
    except ValueError as e:
        logger.warning("Illegal endpoint url format: {} ({})".format(endpoint_url, e))
        raise BadNodeUrlError(endpoint_url)


# TODO: may have many bug
def parse_training_request(req: TCreateTrainingTaskReq):
    """
    Parse TCreateTrainingTaskReq with given yaml template
    Args:
        req: TCreateTrainingTaskReq
    Returns:
        data_conf: configurations related to data
        model_conf: configurations related to model
        task_conf: configurations related to task
    """
    config = req.modelConfigs
    config.update(model_id=req.modelId)
    config.update(tuning=req.isAuto)
    config.update(query_expressions=req.queryExpressions)
    config.update(query_filter=req.queryFilter)
    data_conf_parser, model_conf_parser, task_conf_parser = \
        ConfigParser(), ConfigParser(), ConfigParser()
    data_conf_parser.add_argument('--source_type', type=str, default='file')
    data_conf_parser.add_argument('--filename', type=str, default='dataset/ETT-small/ETTh1.csv')
    data_conf_parser.add_argument('--query_expressions', type=list, default=['root.eg.etth1.s0', 'root.eg.etth1.s1'])
    data_conf_parser.add_argument('--query_filter', type=str, default='-1,1501516800000')
    data_conf_parser.add_argument('--dataset_type', type=str, default='window')
    data_conf_parser.add_argument('--time_embed', type=str, default='h')
    data_conf_parser.add_argument('--input_len', type=int, default=96)
    data_conf_parser.add_argument('--pred_len', type=int, default=96)
    data_conf_parser.add_argument('--input_vars', type=int, default=7)
    data_conf_parser.add_argument('--output_vars', type=int, default=7)

    model_conf_parser.add_argument('--model_name', type=str, default='dlinear')
    model_conf_parser.add_argument('--input_len', type=int, default=96)
    model_conf_parser.add_argument('--pred_len', type=int, default=96)
    model_conf_parser.add_argument('--input_vars', type=int, default=7)
    model_conf_parser.add_argument('--output_vars', type=int, default=7)
    model_conf_parser.add_argument('--task_type', type=str, default='m')
    model_conf_parser.add_argument('--kernel_size', type=int, default=25)
    model_conf_parser.add_argument('--block_type', type=str, default='g')
    model_conf_parser.add_argument('--d_model', type=int, default=128)
    model_conf_parser.add_argument('--inner_layers', type=int, default=4)
    model_conf_parser.add_argument('--outer_layers', type=int, default=4)

    task_conf_parser.add_argument('--model_id', type=str, default='mid_test')
    task_conf_parser.add_argument('--tuning', type=bool, default=False)
    task_conf_parser.add_argument('--task_type', type=str, default='m')
    task_conf_parser.add_argument('--task_class', type=str, default='forecast_training_task')
    task_conf_parser.add_argument('--input_len', type=int, default=96)
    task_conf_parser.add_argument('--pred_len', type=int, default=96)
    task_conf_parser.add_argument('--input_vars', type=int, default=7)
    task_conf_parser.add_argument('--output_vars', type=int, default=7)
    task_conf_parser.add_argument('--learning_rate', type=float, default=0.0001)
    task_conf_parser.add_argument('--batch_size', type=int, default=32)
    task_conf_parser.add_argument('--num_workers', type=int, default=0)
    task_conf_parser.add_argument('--epochs', type=int, default=10)
    task_conf_parser.add_argument('--use_gpu', type=bool, default=False)
    task_conf_parser.add_argument('--gpu', type=int, default=0)
    task_conf_parser.add_argument('--use_multi_gpu', type=bool, default=False)
    task_conf_parser.add_argument('--devices', type=list, default=[0])
    task_conf_parser.add_argument('--metric_names', type=list, default=['MSE', 'MAE'])

    return data_conf_parser.parse_configs(config), \
        model_conf_parser.parse_configs(config), \
        task_conf_parser.parse_configs(config)
