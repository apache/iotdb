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
from iotdb.mlnode.log import logger
from thrift.protocol import TCompactProtocol, TBinaryProtocol
from thrift.transport import TSocket, TTransport
from iotdb.thrift.mlnode import IMLNodeRPCService
from iotdb.thrift.mlnode.ttypes import TCreateTrainingTaskReq, TDeleteModelReq, TForecastReq
from iotdb.thrift.datanode import IDataNodeRPCService
from iotdb.thrift.datanode.ttypes import TFetchTimeseriesReq, TFetchWindowBatchReq, TGroupByTimeParameter, \
    TRecordModelMetricsReq, TDeleteModelMetricsReq
from iotdb.thrift.confignode import IConfigNodeRPCService
from iotdb.thrift.confignode.ttypes import TCreateModelReq, TDropModelReq, TShowModelReq, TShowTrailReq, \
    TUpdateModelInfoReq


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
                             modelId: str = 'test',
                             isAuto: bool = False,
                             modelConfigs: dict = {},
                             queryExpressions: list = [],
                             queryFilter: str = ''):
        req = TCreateTrainingTaskReq(
            modelId=str(modelId),
            isAuto=isAuto,
            modelConfigs={k: str(v) for k, v in modelConfigs.items()},
            queryExpressions=[str(query) for query in queryExpressions],
            queryFilter=str(queryFilter),
        )
        return self.__client.createTrainingTask(req)

    def create_forecast_task(self):
        req = TForecastReq()
        return self.__client.forecast(req)

    def delete_model(self, model_path: str = ''):
        req = TDeleteModelReq(model_path)
        return self.__client.deleteModel(req)


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
                         sessionId=0,
                         statementId=0,
                         queryExpressions: list = [],
                         queryFilter: str = '',
                         fetchSize=20480,
                         timeout=1000):
        req = TFetchTimeseriesReq(
            sessionId=sessionId,
            statementId=statementId,
            queryExpressions=queryExpressions,
            queryFilter=queryFilter,
            fetchSize=fetchSize,
            timeout=timeout
        )
        res = self.__client.fetchTimeseries(req)
        return res

    def record_model_metrics(self, modelID, trialID, metrics, values):
        t = time.time()
        req = TRecordModelMetricsReq(modelId=modelID, trialId=trialID, metrics=metrics, timestamp=t, values=values)
        return self.__client.recordModelMetrics(req)


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

    def update_model_info(self, req: TUpdateModelInfoReq):
        return self.__client.updateModelInfo(req)


dataClient = DataNodeClient(host='127.0.0.1', port=10730)
config_test = {
    'task_class': 'forecast_training_task',
    'source_type': 'thrift',
    'dataset_type': 'window',
    'time_embed': 'h',
    'input_len': 96,
    'pred_len': 96,
    'model_name': 'dlinear',
    'input_vars': 7,
    'output_vars': 7,
    'task_type': 'm',
    'kernel_size': 25,
    'block_type': 'g',
    'd_model': 128,
    'inner_layers': 4,
    'outer_layers': 4,
    'learning_rate': 1e-4,
    'batch_size': 32,
    'num_workers': 0,
    'epochs': 10,
    'metric_name': ['MSE', 'MAE']
}

if __name__ == "__main__":
    # test datanode rpc service
    # client = DataNodeClient(host='127.0.0.1', port=10730)
    # print(dataClient.fetch_timeseries(
    #     queryExpressions=['root.eg.etth1.s0', 'root.eg.etth1.s1'],
    #     queryFilter='-1,1501516800000'
    # ))

    # test mlnode rpc service
    client = MLNodeClient(host="127.0.0.1", port=10810)

    print(client.create_training_task(
        modelId='test',
        isAuto=False,
        modelConfigs=config_test,
        queryExpressions=['root.eg.etth1.**'], # 7 variables
        queryFilter='-1,1501516800000',
    ))
    # print(client.delete_model(model_path='mid_debug/tid_1.pt'))
