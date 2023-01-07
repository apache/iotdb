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


from iotdb.conf.config import *

from src.iotdb.thrift.common.ttypes import (
    TSStatus,
)
from src.iotdb.thrift.mlnode.ttypes import (
    TDeleteModelReq,
    TCreateTrainingTaskReq,
    TForecastReq,
    TForecastResp,
)


from .process.task import BasicTask

class IMLNodeRPCServiceHandler(object):
    def __init__(self):
        pass

    def deleteModel(self, deleteModelReq: TDeleteModelReq):
        modelPath = deleteModelReq.modelPath
        print(modelPath)
        return TSStatus(code=SUCCESS_STATUS)


    def createTrainingTask(self, createTrainingTaskReq: TCreateTrainingTaskReq):
        modelId = createTrainingTaskReq.modelId
        isAuto = createTrainingTaskReq.isAuto
        config = createTrainingTaskReq.modelConfigs
        queryExpressions = createTrainingTaskReq.queryExpressions
        queryFilter = createTrainingTaskReq.queryFilter


        task = BasicTask(config)
        print(task.model)
        print(task.data)

        return TSStatus(code=SUCCESS_STATUS)


    def forecast(self, forecastReq: TForecastReq):
        modelPath = forecastReq.modelPath
        dataset = forecastReq.dataset
        print(modelPath)
        print(dataset)
        status = TSStatus(code=SUCCESS_STATUS)
        forecastResult = b'forecast result'
        return TForecastResp(status, forecastResult)

