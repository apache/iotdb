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
from iotdb.mlnode.data_access.factory import create_forecast_dataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import parse_training_request, parse_forecast_request
from iotdb.mlnode.process.manager import TaskManager
from iotdb.mlnode.storage import model_storage
from iotdb.mlnode.util import get_status
from iotdb.mlnode.config import descriptor
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import (TCreateTrainingTaskReq,
                                        TDeleteModelReq, TForecastReq,
                                        TForecastResp)
from iotdb.mlnode.serde import convert_to_binary


class MLNodeRPCServiceHandler(IMLNodeRPCService.Iface):
    def __init__(self):
        self.__task_manager = TaskManager(pool_size=descriptor.get_config().get_mn_mn_task_pool_size())

    def deleteModel(self, req: TDeleteModelReq):
        try:
            model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warn(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))

    def createTrainingTask(self, req: TCreateTrainingTaskReq):
        task = None
        try:
            # parse request, check required config and config type
            data_config, model_config, task_config = parse_training_request(req)
            # create dataset & check data config legitimacy
            dataset, data_config = create_forecast_dataset(**data_config)

            model_config['input_vars'] = data_config['input_vars']
            model_config['output_vars'] = data_config['output_vars']

            # create task & check task config legitimacy
            task = self.__task_manager.create_training_task(dataset, data_config, model_config, task_config)

            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warn(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))
        finally:
            # submit task stage & check resource and decide pending/start
            self.__task_manager.submit_training_task(task)

    def forecast(self, req: TForecastReq):
        model_path, data, pred_length = parse_forecast_request(req)
        model, model_configs = model_storage.load_model(model_path)
        task_configs = {'pred_len': pred_length}
        try:
            task = self.__task_manager.create_forecast_task(
                task_configs,
                model_configs,
                data,
                model_path
            )
            # submit task stage & check resource and decide pending/start
            forecast_result = convert_to_binary(self.__task_manager.submit_forecast_task(task))
            resp = TForecastResp(get_status(TSStatusCode.SUCCESS_STATUS), forecast_result)
            return resp
        except Exception as e:
            logger.warn(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))
