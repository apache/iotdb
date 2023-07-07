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

from iotdb.mlnode.config import descriptor
from iotdb.mlnode.constant import TSStatusCode
from iotdb.mlnode.data_access.factory import create_dataset
from iotdb.mlnode.data_access.offline.dataset import WindowDataset
from iotdb.mlnode.log import logger
from iotdb.mlnode.parser import parse_forecast_request, parse_task_options, ForecastTaskOptions
from iotdb.mlnode.process.manager import TaskManager
from iotdb.mlnode.serde import convert_to_binary
from iotdb.mlnode.storage import model_storage
from iotdb.mlnode.util import get_status
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
            logger.warn(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))

    def createTrainingTask(self, req: TCreateTrainingTaskReq):
        task = None
        try:
            # parse options
            task_options = parse_task_options(req.options)

            # create task according to task type
            # currently, IoTDB-ML supports forecasting training task only
            task = self.__task_manager.create_forecast_training_task(
                model_id=req.modelId,
                task_options=type(ForecastTaskOptions)(task_options),
                hyperparameters=req.hyperparameters,
                dataset=type(WindowDataset)(create_dataset(req.datasetFetchSQL, task_options))
            )

            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            logger.warn(e)
            return get_status(TSStatusCode.MLNODE_INTERNAL_ERROR, str(e))
        finally:
            if task is not None:
                # submit task to process pool
                self.__task_manager.submit_training_task(task)

    def forecast(self, req: TForecastReq):
        model_path, data, pred_length = parse_forecast_request(req)
        try:
            task = self.__task_manager.create_forecast_task(
                pred_length,
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
