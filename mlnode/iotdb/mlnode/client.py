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


import time

from thrift.protocol import TBinaryProtocol, TCompactProtocol
from thrift.transport import TSocket, TTransport

from iotdb.mlnode.config import config
from iotdb.mlnode.log import logger
from iotdb.thrift.common.ttypes import TEndPoint
from iotdb.thrift.confignode import IConfigNodeRPCService
from iotdb.thrift.confignode.ttypes import (TCreateModelReq, TDropModelReq,
                                            TShowModelReq, TShowTrailReq,
                                            TUpdateModelInfoReq)
from iotdb.thrift.datanode import IDataNodeRPCService
from iotdb.thrift.datanode.ttypes import (TDeleteModelMetricsReq,
                                          TFetchTimeseriesReq,
                                          TFetchWindowBatchReq,
                                          TGroupByTimeParameter,
                                          TRecordModelMetricsReq)
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import (TCreateTrainingTaskReq,
                                        TDeleteModelReq, TForecastReq)


class ClientManager(object):
    def __init__(self,
                 config_node_address: TEndPoint,
                 data_node_address: TEndPoint):
        self.data_node_address = data_node_address
        self.config_node_address = config_node_address

    def borrow_data_node_client(self):
        return DataNodeClient(host=self.data_node_address.ip,
                              port=self.data_node_address.port)

    def borrow_config_node_client(self):
        return DataNodeClient(host=self.config_node_address.ip,
                              port=self.config_node_address.port)


class MLNodeClient(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port

        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        protocol = TCompactProtocol.TCompactProtocol(transport)
        self.__client = IMLNodeRPCService.Client(protocol)

    def create_training_task(self,
                             model_id: str = 'test',
                             is_auto: bool = False,
                             model_configs: dict = {},
                             query_expressions: list = [],
                             query_filter: str = ''):
        req = TCreateTrainingTaskReq(
            modelId=model_id,
            isAuto=is_auto,
            modelConfigs={k: str(v) for k, v in model_configs.items()},
            queryExpressions=[str(query) for query in query_expressions],
            queryFilter=query_filter,
        )
        return self.__client.createTrainingTask(req)

    def create_forecast_task(self):
        req = TForecastReq()
        return self.__client.forecast(req)

    def delete_model(self,
                     model_id: str = '',
                     trial_id: str = ''):
        req = TDeleteModelReq(modelId=model_id, trialId=trial_id)
        res = self.__client.deleteModel(req)
        return res


class DataNodeClient(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port

        transport = TTransport.TFramedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.__client = IDataNodeRPCService.Client(protocol)

    def fetch_timeseries(self,
                         session_id: int = 0,
                         statement_id: int = 0,
                         query_expressions: list = [],
                         query_filter: str = '',
                         fetch_size: int = 1024,
                         timeout: int = 1000):
        req = TFetchTimeseriesReq(
            sessionId=session_id,
            statementId=statement_id,
            queryExpressions=query_expressions,
            queryFilter=query_filter,
            fetchSize=fetch_size,
            timeout=timeout
        )
        res = self.__client.fetchTimeseries(req)
        return res

    def record_model_metrics(self,
                             model_id: str = '',
                             trial_id: str = '',
                             metrics: list = [],
                             timestamp: int = -1,  # default: current time in ms
                             values: list = []):
        timestamp = int(time.time()) * 1000 if timestamp == -1 else timestamp
        req = TRecordModelMetricsReq(
            modelId=model_id,
            trialId=trial_id,
            metrics=metrics,
            timestamp=timestamp,
            values=values
        )
        res = self.__client.recordModelMetrics(req)
        return res


class ConfigNodeClient(object):
    def __init__(self, host, port):
        self.__host = host
        self.__port = port

        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        protocol = TCompactProtocol.TCompactProtocol(transport)
        self.__client = IConfigNodeRPCService.Client(protocol)

    def update_model_info(self,
                          model_id: str = '',
                          trial_id: str = '',
                          model_info: dict = {}):
        req = TUpdateModelInfoReq(
            modelId=model_id,
            trialId=trial_id,
            modelInfo={k: str(v) for k, v in model_info.items()},
        )
        res = self.__client.updateModelInfo(req)
        return res


client_manager = ClientManager(
    config_node_address=config.get_mn_target_config_node(),
    data_node_address=config.get_mn_target_data_node()
)
