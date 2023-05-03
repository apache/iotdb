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
from iotdb.mlnode.algorithm.factory import create_forecast_model
from iotdb.mlnode.constant import TSStatusCode
from iotdb.mlnode.data_access.factory import create_forecast_dataset
from iotdb.mlnode.parser import parse_training_request
from iotdb.mlnode.process.manager import TaskManager
from iotdb.mlnode.storage import model_storage
from iotdb.mlnode.util import get_status
from iotdb.mlnode.config import descriptor
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import (TCreateTrainingTaskReq,
                                        TDeleteModelReq, TForecastReq,
                                        TForecastResp)


class MLNodeRPCServiceHandler(IMLNodeRPCService.Iface):
    def __init__(self):
        self.__task_manager = TaskManager(pool_size=descriptor.get_config().get_mn_mn_task_pool_size())

    def deleteModel(self, req: TDeleteModelReq):
        try:
            model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
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

            # create model & check model config legitimacy
            # model, model_config = create_forecast_model(**model_config)

            model_config['input_vars'] = data_config['input_vars']
            model_config['output_vars'] = data_config['output_vars']
            # create task & check task config legitimacy
            task = self.__task_manager.create_training_task(dataset, data_config, model_config, task_config)

            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))
        finally:
            # submit task stage & check resource and decide pending/start
            self.__task_manager.submit_training_task(task)

    def forecast(self, req: TForecastReq):
        status = get_status(TSStatusCode.SUCCESS_STATUS)
        forecast_result = b'forecast result'
        return TForecastResp(status, forecast_result)
