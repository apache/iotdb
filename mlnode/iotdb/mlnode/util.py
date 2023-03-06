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


def parse_training_request(req: TCreateTrainingTaskReq):  # TODO: extend for other request
    """
    Parse TCreateTrainingTaskReq with given yaml template

    Args:
        req: TCreateTrainingTaskReq

    Returns:
        data_conf: configurations related to data
        model_conf: configurations related to model
        trial_conf: configurations related to training
    """
    config = req.modelConfigs
    config.update(model_id=req.modelId)
    config.update(query_expressions=str(req.queryExpressions))
    config.update(queue_filter=req.queryFilter)
    # config = {k: eval(v) for k, v in config.items()}

    yaml_path = os.path.join(MLNODE_REQUEST_TEMPLATE, 'createTrainingTask.yml')
    with open(yaml_path, 'r') as f:  # TODO: add exception check
        default_confs = yaml.safe_load_all(f)
        data_conf, model_conf, trial_conf = tuple(default_confs)

    for k, v in config.items():
        if k in data_conf.keys():
            data_conf[k] = v if type(data_conf[k]) is str else eval(v)
        if k in model_conf.keys():
            model_conf[k] = v if type(model_conf[k]) is str else eval(v)
        if k in trial_conf.keys():
            trial_conf[k] = v if type(trial_conf[k]) is str else eval(v)

    return data_conf, model_conf, trial_conf
