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


import os
import yaml
from iotdb.mlnode.exception import BadNodeUrlError
from iotdb.mlnode.log import logger
from iotdb.thrift.common.ttypes import TEndPoint
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq
from iotdb.mlnode.constant import MLNODE_REQUEST_TEMPLATE
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
    config.update(metric_names=eval(config['metric_names']))
    data_conf_parser = ConfigParser('data')
    data_conf_keys = vars(data_conf_parser.parse_args([])).keys()
    model_conf_parser = ConfigParser('model')
    model_conf_keys = vars(model_conf_parser.parse_args([])).keys()
    task_conf_parser = ConfigParser('task')
    task_conf_keys = vars(task_conf_parser.parse_args([])).keys()
    data_args = []
    model_args = []
    task_args = []
    for k, v in config.items():
        if k in data_conf_keys:
            data_args.append('--' + k)
            data_args.append(v)
        if k in model_conf_keys:
            model_args.append('--' + k)
            model_args.append(v)
        if k in task_conf_keys:
            task_args.append('--' + k)
            task_args.append(v)
    data_conf = vars(data_conf_parser.parse_args(data_args))
    model_conf = vars(model_conf_parser.parse_args(model_args))
    task_conf = vars(task_conf_parser.parse_args(task_args))
    return data_conf, model_conf, task_conf


if __name__ == '__main__':
    from client import *
    modelId = 'mid_etth1_dlinear_default'
    isAuto = False
    modelConfigs = config_dlinear
    queryExpressions = ['root.eg.etth1.**', 'root.eg.etth1.**', 'root.eg.etth1.**']
    queryFilter = '0,1501516800000'
    req = TCreateTrainingTaskReq(
        modelId=str(modelId),
        isAuto=isAuto,
        modelConfigs={k: str(v) for k, v in modelConfigs.items()},
        queryExpressions=[str(query) for query in queryExpressions],
        queryFilter=str(queryFilter),
    )
    data_conf, model_conf, task_conf = parse_training_request(req)
    print(data_conf)
    print(model_conf)
    print(task_conf)
