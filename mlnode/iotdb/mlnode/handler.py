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
from iotdb.mlnode.util import parse_training_request
from debug import *


class TSStatusCode(Enum):
    SUCCESS_STATUS = 200

    def get_status_code(self) -> int:
        return self.value


def get_status(status_code: TSStatusCode, message: str) -> TSStatus:
    status = TSStatus(status_code.get_status_code())
    status.message = message
    return status


class MLNodeRPCServiceHandler(IMLNodeRPCService.Iface):
    def __init__(self):
        self.taskManager = Manager(10)
        pass

    def delete_model(self, req: TDeleteModelReq):
        #TODO
        return get_status(TSStatusCode.SUCCESS_STATUS, "")

    def create_training_task(self, req: TCreateTrainingTaskReq):
        # TODO: parse_request
        # config = parse_training_request()
        config = debug_model_config(), debug_data_config(), debug_trial_config()
        self.taskManager.create_single_training_task_pool(config)
        return get_status(TSStatusCode.SUCCESS_STATUS, "")

    # def forecast(self, req: TForecastReq):
    #     status = get_status(TSStatusCode.SUCCESS_STATUS, "")
    #     forecast_result = b'forecast result'
    #     self.taskManager.create_inference_task_pool(default_configs())
    #     return TForecastResp(status, forecast_result)
