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

from iotdb.mlnode.constant import TSStatusCode
from iotdb.mlnode.exception import BadNodeUrlError
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import ConfigParser
from iotdb.thrift.common.ttypes import TEndPoint, TSStatus
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq


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


def get_status(status_code: TSStatusCode, message: str) -> TSStatus:
    status = TSStatus(status_code.get_status_code())
    status.message = message
    return status


def verify_success(status: TSStatus, err_msg: str) -> None:
    if status.code != TSStatusCode.SUCCESS_STATUS:
        logger.warn(err_msg + ", error status is ", status)
        raise RuntimeError(str(status.code) + ": " + status.message)


def parse_training_request(req: TCreateTrainingTaskReq):
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
    config.update(model_id=req.modelId)
    config.update(tuning=req.isAuto)
    config.update(query_expressions=req.queryExpressions)
    config.update(query_filter=req.queryFilter)

    data_config = _get_data_config_parser().parse_configs(config)
    model_config = _get_model_config_parser().parse_configs(config)
    task_config = _get_task_config_parser().parse_configs(config)
    return data_config, model_config, task_config


def _get_data_config_parser():
    """
    Get data config parser
    Returns:
        data_config_parser: parser for data config
    Argument description:
        query_expressions: query expressions
        query_filter: query filter
        source_type: source type
        filename: filename
        dataset_type: dataset type
        time_embed: freq for time features encoding
        input_len: input sequence length
        pred_len: prediction sequence length
        input_vars: number of input variables
        output_vars: number of output variables
    """
    data_config_parser = ConfigParser()
    data_config_parser.add_argument('--source_type', type=str, required=True)
    data_config_parser.add_argument('--dataset_type', type=str, required=True)
    data_config_parser.add_argument('--filename', type=str, default='')
    data_config_parser.add_argument('--query_expressions', type=str, nargs='*', default=[])
    data_config_parser.add_argument('--query_filter', type=str, default='')
    data_config_parser.add_argument('--time_embed', type=str, default='h')
    data_config_parser.add_argument('--input_len', type=int, default=96)
    data_config_parser.add_argument('--pred_len', type=int, default=96)
    data_config_parser.add_argument('--input_vars', type=int, default=1)
    data_config_parser.add_argument('--output_vars', type=int, default=1)
    return data_config_parser


def _get_model_config_parser():
    """
    Get model config parser
    Returns:
        model_config_parser: parser for model config
    Argument description:
        model_name: model name
        input_len: input sequence length
        pred_len: prediction sequence length
        input_vars: number of input variables
        output_vars: number of output variables
        task_type: task type, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate,
            MS:multivariate predict univariate'
        kernel_size: kernel size
        block_type: block type
        d_model: dimension of feature in model
        inner_layers: number of inner layers
        outer_layers: number of outer layers
    """
    model_config_parser = ConfigParser()
    model_config_parser.add_argument('--model_name', type=str, required=True)
    model_config_parser.add_argument('--input_len', type=int, default=96)
    model_config_parser.add_argument('--pred_len', type=int, default=96)
    model_config_parser.add_argument('--input_vars', type=int, default=1)
    model_config_parser.add_argument('--output_vars', type=int, default=1)
    model_config_parser.add_argument('--forecast_type', type=str, default='m')
    model_config_parser.add_argument('--kernel_size', type=int, default=25)
    model_config_parser.add_argument('--block_type', type=str, default='generic')
    model_config_parser.add_argument('--d_model', type=int, default=128)
    model_config_parser.add_argument('--inner_layers', type=int, default=4)
    model_config_parser.add_argument('--outer_layers', type=int, default=4)
    return model_config_parser


def _get_task_config_parser():
    """
    Get task config parser
    Returns:
        task_config_parser: parser for task config
    Argument description:
        model_id: model id
        tuning: whether to tune hyperparameters
        task_type: task type, options:[M, S, MS]; M:multivariate predict multivariate, S:univariate predict univariate,
            MS:multivariate predict univariate'
        task_class: task class
        input_len: input sequence length
        pred_len: prediction sequence length
        input_vars: number of input variables
        output_vars: number of output variables
        learning_rate: learning rate
        batch_size: batch size
        num_workers: number of workers
        epochs: number of epochs
        use_gpu: whether to use gpu
        use_multi_gpu: whether to use multi-gpu
        devices: devices to use
        metric_names: metric to use
    """
    task_config_parser = ConfigParser()
    task_config_parser.add_argument('--task_class', type=str, required=True)
    task_config_parser.add_argument('--model_id', type=str, required=True)
    task_config_parser.add_argument('--tuning', type=bool, default=False)
    task_config_parser.add_argument('--forecast_type', type=str, default='m')
    task_config_parser.add_argument('--input_len', type=int, default=96)
    task_config_parser.add_argument('--pred_len', type=int, default=96)
    task_config_parser.add_argument('--input_vars', type=int, default=1)
    task_config_parser.add_argument('--output_vars', type=int, default=1)
    task_config_parser.add_argument('--learning_rate', type=float, default=0.0001)
    task_config_parser.add_argument('--batch_size', type=int, default=32)
    task_config_parser.add_argument('--num_workers', type=int, default=0)
    task_config_parser.add_argument('--epochs', type=int, default=10)
    task_config_parser.add_argument('--use_gpu', type=bool, default=False)
    task_config_parser.add_argument('--gpu', type=int, default=0)
    task_config_parser.add_argument('--use_multi_gpu', type=bool, default=False)
    task_config_parser.add_argument('--devices', type=int, nargs='+', default=[0])
    task_config_parser.add_argument('--metric_names', type=str, nargs='+', default=['MSE', 'MAE'])
    return task_config_parser
