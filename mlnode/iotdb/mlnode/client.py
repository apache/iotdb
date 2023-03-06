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
from thrift.protocol import TCompactProtocol
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
                             modelId: int = -1,
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

        transport = TTransport.TBufferedTransport(
            TSocket.TSocket(self.__host, self.__port)
        )
        if not transport.isOpen():
            try:
                transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        protocol = TCompactProtocol.TCompactProtocol(transport)
        self.__client = IDataNodeRPCService.Client(protocol)

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


config_test = {
    'model_id': 0,
    'source_type': 'file',
    'filename': 'dataset/exchange_rate/exchange_rate.csv',
    # 'ip': '127.0.0.1',
    # 'port': '6667',
    # 'username': 'root',
    # 'password': 'root',
    # 'sql': {
    #     'train': 'SELECT * FROM root.eg.etth1 WHERE Time<=2017-08-01',
    #     'val': 'SELECT * FROM root.eg.etth1 WHERE Time>2017-08-01 and Time<2018-01-01',
    #     'test': 'SELECT * FROM root.eg.etth1 WHERE Time>=2018-01-01',
    # },
    'query_expressions': [],
    'query_filter': '',
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
    'metric_name': ['MSE', 'MAE'],
}

if __name__ == "__main__":
    # test rpc service
    client = MLNodeClient(host="127.0.0.1", port=10810)

    print(client.create_training_task(
        modelId=1,
        modelConfigs=config_test,
    ))
    # client.delete_model(model_path='')
