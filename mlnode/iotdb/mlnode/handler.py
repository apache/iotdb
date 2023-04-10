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
                                        TDeleteModelReq, TForecastReq,
                                        TForecastResp)


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
        pass

    def deleteModel(self, req: TDeleteModelReq):
        try:
            model_storage.delete_model(req.modelId)
            return get_status(TSStatusCode.SUCCESS_STATUS)
        except Exception as e:
            return get_status(TSStatusCode.FAIL_STATUS, str(e))

    def createTrainingTask(self, req: TCreateTrainingTaskReq):
        return get_status(TSStatusCode.SUCCESS_STATUS, "")

    def forecast(self, req: TForecastReq):
        status = get_status(TSStatusCode.SUCCESS_STATUS, "")
        forecast_result = b'forecast result'
        return TForecastResp(status, forecast_result)
