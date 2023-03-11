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


from enum import Enum
from iotdb.thrift.common.ttypes import TSStatus
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import (TCreateTrainingTaskReq,
                                        TDeleteModelReq,
                                        TForecastReq,
                                        TForecastResp)
from iotdb.mlnode.manager import Manager
from iotdb.mlnode.log import logger
from iotdb.mlnode.util import parse_training_request
from iotdb.mlnode.storage.model_storager import modelStorager


class TSStatusCode(Enum):
    SUCCESS_STATUS = 200
    FAIL_STATUS = 400

    def get_status_code(self) -> int:
        return self.value


def get_status(status_code: TSStatusCode, message: str) -> TSStatus:
    status = TSStatus(status_code.get_status_code())
    status.message = message
    return status


class MLNodeRPCServiceHandler(IMLNodeRPCService.Iface):
    def __init__(self):
        self.taskManager = Manager(10)

    def deleteModel(self, req: TDeleteModelReq):
        model_path = req.modelPath
        if modelStorager.delete_by_path(model_path):
            return get_status(TSStatusCode.SUCCESS_STATUS, "Successfully delete")
        else:
            return get_status(TSStatusCode.FAIL_STATUS, "Fail to delete")

    def createTrainingTask(self, req: TCreateTrainingTaskReq):
        data_conf, model_conf, task_conf = parse_training_request(req)  # data_conf, model_conf, trial_conf
        # print(data_conf, model_conf, task_conf)
        self.taskManager.submit_training_task(data_conf, model_conf, task_conf) # TODO, check param and decide the response
        return get_status(TSStatusCode.SUCCESS_STATUS, "create training task successfully")

    # def forecast(self, req: TForecastReq):
    #     status = get_status(TSStatusCode.SUCCESS_STATUS, "")
    #     forecast_result = b'forecast result'
    #     self.taskManager.create_inference_task_pool(default_configs())
    #     return TForecastResp(status, forecast_result)
